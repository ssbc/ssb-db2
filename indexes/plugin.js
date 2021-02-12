const Obv = require('obz')
const Level = require('level')
const debounce = require('lodash.debounce')
const path = require('path')
const Debug = require('debug')
const DeferredPromise = require('p-defer')
const { indexesPath } = require('../defaults')

module.exports = function (
  dir,
  name,
  version,
  handleData,
  writeData,
  beforeIndexUpdate
) {
  const indexPath = path.join(indexesPath(dir), name)
  const debug = Debug('ssb:db2:' + name)

  if (typeof window === 'undefined') {
    // outside browser
    const mkdirp = require('mkdirp')
    mkdirp.sync(indexPath)
  }

  const level = Level(indexPath)
  const META = '\x00'
  const chunkSize = 2048
  let processed = 0 // processed "seq"
  const offset = Obv() // persisted
  const stateLoaded = DeferredPromise()
  let notPersistedOffset = -1

  function writeBatch(cb) {
    if (notPersistedOffset > -1 && !level.isClosed()) {
      writeData((err) => {
        if (err) return cb(err)

        // we can't batch this as the encoding might not be the same as the plugin
        if (!level.isClosed()) {
          level.put(
            META,
            { version, offset: notPersistedOffset, processed },
            { valueEncoding: 'json' },
            (err) => {
              if (err) cb(err)
              else {
                offset.set(notPersistedOffset)
                cb()
              }
            }
          )
        }
      })
    } else cb()
  }

  const liveWriteBatch = debounce(writeBatch, 250)

  function onData(record, isLive) {
    let changes = 0
    if (record.offset > offset.value) {
      changes = handleData(record, processed)
      processed++
    }
    notPersistedOffset = record.offset

    if (changes > chunkSize) writeBatch(() => {})
    else if (isLive) liveWriteBatch(() => {})
  }

  level.get(META, { valueEncoding: 'json' }, (err, data) => {
    debug(`got index status:`, data)

    if (data && data.version == version) {
      processed = data.processed
      if (beforeIndexUpdate) {
        beforeIndexUpdate(() => {
          offset.set(data.offset)
          stateLoaded.resolve()
        })
      } else {
        offset.set(data.offset)
        stateLoaded.resolve()
      }
    } else {
      level.clear(() => {
        offset.set(-1)
        stateLoaded.resolve()
      })
    }
  })

  return { level, offset, onData, writeBatch, stateLoaded: stateLoaded.promise }
}
