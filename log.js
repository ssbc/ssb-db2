const OffsetLog = require('async-flumelog')
const bipf = require('bipf')
const { BLOCK_SIZE, newLogPath } = require('./defaults')

module.exports = function (dir, config) {
  config = config || {}

  const log = OffsetLog(newLogPath(dir), {
    blockSize: BLOCK_SIZE,
  })

  log.add = function (id, msg, cb) {
    const data = {
      key: id,
      value: msg,
      timestamp: Date.now(),
    }
    const b = Buffer.alloc(bipf.encodingLength(data))
    bipf.encode(data, b, 0)
    log.append(b, function (err) {
      if (err) cb(err)
      else cb(null, data)
    })
  }

  return log
}
