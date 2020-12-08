const Obv = require('obv')
const Level = require('level')
const path = require('path')
const Debug = require('debug')
const { indexesPath } = require('../defaults')

module.exports = function (
  log,
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
  let processed = 0
  const seq = Obv()
  let unWrittenSeq = -1

  function writeBatch(cb) {
    if (unWrittenSeq > -1) {
      level.put(
        META,
        { version, seq: unWrittenSeq, processed },
        { valueEncoding: 'json' },
        (err) => {
          if (err) throw err
        }
      )

      writeData((err) => {
        if (err) return cb(err)
        else {
          seq.set(unWrittenSeq)
          cb()
        }
      })
    } else cb()
  }

  function onData(data, isLive) {
    // maybe check if for us!
    let unwritten = handleData(data, processed)
    if (unwritten > 0) unWrittenSeq = data.seq
    processed++

    if (unwritten > chunkSize || isLive) writeBatch(() => {})
  }

  level.get(META, { valueEncoding: 'json' }, (err, data) => {
    debug(`got index status:`, data)

    if (data && data.version == version) {
      processed = data.processed
      if (beforeIndexUpdate)
        beforeIndexUpdate(() => {
          seq.set(data.seq)
        })
      else seq.set(data.seq)
    } else
      level.clear(() => {
        seq.set(-1)
      })
  })

  return { level, seq, onData, writeBatch }
}
