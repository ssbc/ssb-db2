const Obv = require('obv')
const Level = require('level')
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
  const offset = Obv()
  const stateLoaded = DeferredPromise()
  let unWrittenOffset = -1

  function writeBatch(cb) {
    if (unWrittenOffset > -1 && !level.isClosed()) {
      level.put(
        META,
        { version, offset: unWrittenOffset, processed },
        { valueEncoding: 'json' },
        (err) => {
          if (err) throw err
        }
      )

      writeData((err) => {
        if (err) return cb(err)
        else {
          offset.set(unWrittenOffset)
          cb()
        }
      })
    } else cb()
  }

  function onData(record, isLive) {
    // maybe check if for us!
    let unwritten = handleData(record, processed)
    if (unwritten > 0) unWrittenOffset = record.offset
    processed++

    if (unwritten > chunkSize || isLive) writeBatch(() => {})
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
