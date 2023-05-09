// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const Obv = require('obz')
const Level = require('level')
const debounce = require('lodash.debounce')
const encodings = require('level-codec/lib/encodings')
const path = require('path')
const Debug = require('debug')
const DeferredPromise = require('p-defer')
const { indexesPath } = require('../defaults')

const notInABrowser = typeof window === 'undefined'
let rimraf
let mkdirp
if (notInABrowser) {
  rimraf = require('rimraf')
  mkdirp = require('mkdirp')
}

function thenMaybeReportError(err) {
  if (err) console.error(err)
}

module.exports = class Plugin {
  constructor(log, dir, name, version, keyEncoding, valueEncoding, configDb2) {
    this.log = log
    this.name = name
    this.levelPutListeners = []
    this.levelDelListeners = []
    this.levelBatchListeners = []
    this._keyEncoding = keyEncoding
    this._valueEncoding = valueEncoding
    this._indexPath = path.join(indexesPath(dir), name)
    const debug = Debug('ssb:db2:' + name)

    if (notInABrowser) mkdirp.sync(this._indexPath)
    this.level = Level(this._indexPath)

    const META = '\x00'
    const chunkSize = 2048
    let processedSeq = 0
    let processedOffset = -1
    this.offset = Obv() // persisted offset
    this._stateLoaded = DeferredPromise()
    this.batch = []

    /**
     * Internal flush() method. Do not call this from other modules.
     */
    this._flush = (overwriting, cb) => {
      if (this.level.isClosed()) return cb()
      if (!overwriting && processedOffset === this.offset.value) return cb()
      if (!this.onFlush) this.onFlush = (cb2) => cb2()

      const processedOffsetAtFlush = processedOffset
      const processedSeqAtFlush = processedSeq

      this.onFlush((err) => {
        if (err) return cb(err)

        // 1st, persist the operations in the batch array
        this.level.batch(
          this.batch,
          { keyEncoding: this.keyEncoding, valueEncoding: this.valueEncoding },
          (err2) => {
            // prettier-ignore
            if (err2) return cb(new Error('failed to persist operations when flushing', {cause: err2}))
            if (this.level.isClosed()) return cb()

            // 2nd, persist the META because it has its own valueEncoding
            this.level.put(
              META,
              {
                version,
                offset: processedOffsetAtFlush,
                processed: processedSeqAtFlush,
              },
              { valueEncoding: 'json' },
              (err3) => {
                // prettier-ignore
                if (err3) cb(new Error('failed to persist META when flushing', {cause: err3}))
                else {
                  if (processedOffsetAtFlush === this.log.since.value) {
                    this.enableLive()
                  }
                  this.offset.set(processedOffsetAtFlush)
                  cb()
                }
              }
            )
          }
        )
        this.batch = []
      })
    }

    /**
     * Flush the batched operations such that leveldb is written to disk, only
     * if this index's offset has moved forwards.
     */
    this.flush = this._flush.bind(this, false)

    /**
     * Flush the batched operations such that leveldb is written to disk, even
     * if this index's offset hasn't changed (thus we're overwriting old items).
     */
    this.forcedFlush = this._flush.bind(this, true)

    const flushDebounce = (configDb2 && configDb2.flushDebounce) || 250
    const liveFlush = debounce(this.flush, flushDebounce)

    this.onRecord = function onRecord(record, isLive, pValue) {
      let changes = 0
      if (record.offset > processedOffset) {
        if (record.value && pValue >= 0) {
          this.processRecord(record, processedSeq, pValue)
        }
        changes = this.batch.length
        processedSeq++
        processedOffset = record.offset
      }

      if (changes > chunkSize) this.flush(thenMaybeReportError)
      else if (isLive) liveFlush(thenMaybeReportError)
    }

    this.level.get(META, { valueEncoding: 'json' }, (err, status) => {
      debug(`got index status:`, status)

      if (status && status.version === version) {
        processedSeq = status.processed
        processedOffset = status.offset
        this.offset.set(status.offset)
        if (this.onLoaded) {
          this.onLoaded(() => {
            this._stateLoaded.resolve()
          })
        } else {
          this._stateLoaded.resolve()
        }
      } else {
        this.level.clear(() => {
          // processedSeq & processedOffset have proper defaults

          this.offset.set(-1)
          this._stateLoaded.resolve()
        })
      }
    })

    const subClassReset = this.reset
    this.reset = (cb) => {
      this.disableLive()
      if (subClassReset) subClassReset.call(this)
      this.batch = []
      this.offset.set(-1)
      this.clear(function levelPluginCleared() {
        processedSeq = 0
        processedOffset = -1
        cb()
      })
    }
  }

  /**
   * Leveldown clear() is notoriously slow, because it does something naive:
   * it iterates over each db item (in JS!) and deletes one *at a time*. Not
   * even parallelism is employed.
   *
   * So we implement clear ourselves with a nuclear approach: delete the folder
   * and recreate level.
   */
  clear(cb) {
    if (notInABrowser) {
      this.level.close(() => {
        rimraf.sync(this._indexPath)
        mkdirp.sync(this._indexPath)
        this.level = Level(this._indexPath)
        cb()
      })
    } else {
      this.level.clear(cb)
    }
  }

  get stateLoaded() {
    return this._stateLoaded.promise
  }

  // The reason why we need this is that `pull-level` (often used to read these
  // level indexes) only supports objects of shape {encode,decode,type,buffer}
  // for `keyEncoding` and `valueEncoding`. Actually, it's `level-post` that
  // doesn't support it, but `level-post` is a dependency in `pull-level`.
  // Note, this._keyEncoding and this._valueEncoding are strings.
  get keyEncoding() {
    if (encodings[this._keyEncoding]) return encodings[this._keyEncoding]
    else return undefined
  }

  get valueEncoding() {
    if (encodings[this._valueEncoding]) return encodings[this._valueEncoding]
    else return undefined
  }

  close(cb) {
    this.level.close(cb)
  }

  processRecord() {
    throw new Error('processRecord() is missing an implementation')
  }

  disableLive() {
    this.levelPutListeners = this.level.rawListeners('put')
    this.levelDelListeners = this.level.rawListeners('del')
    this.levelBatchListeners = this.level.rawListeners('batch')
    this.level.removeAllListeners('put')
    this.level.removeAllListeners('del')
    this.level.removeAllListeners('batch')
  }

  enableLive() {
    for (const fn of this.levelPutListeners) this.level.on('put', fn)
    for (const fn of this.levelDelListeners) this.level.on('del', fn)
    for (const fn of this.levelBatchListeners) this.level.on('batch', fn)
    this.levelPutListeners.length = 0
    this.levelDelListeners.length = 0
    this.levelBatchListeners.length = 0
  }

  // used for reindexing encrypted content
  indexesContent() {
    return true
  }
}
