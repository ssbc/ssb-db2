const Obv = require('obz')
const Level = require('level')
const debounce = require('lodash.debounce')
const path = require('path')
const Debug = require('debug')
const DeferredPromise = require('p-defer')
const { indexesPath } = require('../defaults')

module.exports = class Plugin {
  constructor(dir, name, version, keyEncoding, valueEncoding) {
    this.name = name
    this.keyEncoding = keyEncoding
    this.valueEncoding = valueEncoding
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
    let processed = 0 // processed seq
    this.offset = Obv() // persisted offset
    this._stateLoaded = DeferredPromise()
    let notPersistedOffset = -1
    this.batch = []

    this.flush = (cb) => {
      if (notPersistedOffset < 0 || this.level.isClosed()) return cb()

      this.flushBatch((err) => {
        if (err) return cb(err)
        if (this.level.isClosed()) return cb()

        // we can't batch this as the valueEncoding might be different
        this.level.put(
          META,
          { version, offset: notPersistedOffset, processed },
          { valueEncoding: 'json' },
          (err) => {
            if (err) cb(err)
            else {
              this.offset.set(notPersistedOffset)
              cb()
            }
          }
        )
      })
    }

    const liveFlush = debounce(this.flush, 250)

    this.onData = function onData(record, isLive) {
      let changes = 0
      if (record.offset > this.offset.value) {
        if (record.value) this.handleRecord(record, processed)
        changes = this.batch.length
        processed++
      }
      notPersistedOffset = record.offset

      if (changes > chunkSize) this.flush(() => {})
      else if (isLive) liveFlush(() => {})
    }

    this.level.get(META, { valueEncoding: 'json' }, (err, data) => {
      debug(`got index status:`, data)

      if (data && data.version === version) {
        processed = data.processed
        if (this.beforeIndexUpdate) {
          this.beforeIndexUpdate(() => {
            this.offset.set(data.offset)
            this._stateLoaded.resolve()
          })
        } else {
          this.offset.set(data.offset)
          this._stateLoaded.resolve()
        }
      } else {
        this.level.clear(() => {
          this.offset.set(-1)
          this._stateLoaded.resolve()
        })
      }
    })
  }

  get stateLoaded() {
    return this._stateLoaded.promise
  }

  remove(...args) {
    this.level.clear(...args)
  }

  close(cb) {
    this.level.close(cb)
  }

  handleRecord() {
    throw new Error('handleRecord() is missing an implementation')
  }

  flushBatch(cb) {
    this.level.batch(
      this.batch,
      { keyEncoding: this.keyEncoding, valueEncoding: this.valueEncoding },
      cb
    )
    this.batch = []
  }
}
