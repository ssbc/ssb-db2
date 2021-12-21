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

function thenMaybeReportError(err) {
  if (err) console.error(err)
}

module.exports = class Plugin {
  constructor(log, dir, name, version, keyEncoding, valueEncoding) {
    this.log = log
    this.name = name
    this._keyEncoding = keyEncoding
    this._valueEncoding = valueEncoding
    const debug = Debug('ssb:db2:' + name)

    const indexPath = path.join(indexesPath(dir), name)
    if (typeof window === 'undefined') {
      // outside browser
      const mkdirp = require('mkdirp')
      mkdirp.sync(indexPath)
    }
    this.level = Level(indexPath)

    const META = '\x00'
    const chunkSize = 2048
    let processedSeq = 0
    let processedOffset = -1
    this.offset = Obv() // persisted offset
    this._stateLoaded = DeferredPromise()
    this.batch = []

    this.flush = (cb) => {
      if (processedOffset === this.offset.value || this.level.isClosed()) return cb()
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
            if (err2) return cb(err2)
            if (this.level.isClosed()) return cb()

            // 2nd, persist the META because it has its own valueEncoding
            this.level.put(
              META,
              { version, offset: processedOffsetAtFlush, processed: processedSeqAtFlush },
              { valueEncoding: 'json' },
              (err3) => {
                if (err3) cb(err3)
                else {
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

    const liveFlush = debounce(this.flush, 250)

    this.onRecord = function onRecord(record, isLive) {
      let changes = 0
      if (record.offset > processedOffset) {
        if (record.value) this.processRecord(record, processedSeq)
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
          processedOffset = -1
          this.offset.set(-1)
          this._stateLoaded.resolve()
        })
      }
    })
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

  remove(...args) {
    this.level.clear(...args)
  }

  close(cb) {
    this.level.close(cb)
  }

  processRecord() {
    throw new Error('processRecord() is missing an implementation')
  }

  // used for reindexing encrypted content
  indexesContent() {
    return true
  }
}
