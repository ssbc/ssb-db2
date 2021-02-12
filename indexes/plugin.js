const Obv = require('obz')
const Level = require('level')
const debounce = require('lodash.debounce')
const path = require('path')
const Debug = require('debug')
const DeferredPromise = require('p-defer')
const { indexesPath } = require('../defaults')

module.exports = class Plugin {
  constructor(dir, name, version) {
    this.name = name
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

    this.writeBatch = function writeBatch(cb) {
      if (notPersistedOffset > -1 && !this.level.isClosed()) {
        this.flushBatch((err) => {
          if (err) return cb(err)

          // we can't batch this as the encoding might not be the same as the plugin
          if (!this.level.isClosed()) {
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
          }
        })
      } else cb()
    }

    const liveWriteBatch = debounce(this.writeBatch.bind(this), 250)

    this.onData = function onData(record, isLive) {
      this.handleData(record, processed)
      notPersistedOffset = record.offset
      processed++

      if (this.batch.length > chunkSize) this.writeBatch(() => {})
      else if (isLive) liveWriteBatch(() => {})
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

  handleData() {
    throw new Error('handleData() is missing an implementation')
  }

  flushBatch() {
    throw new Error('flushBatch() is missing an implementation')
  }
}
