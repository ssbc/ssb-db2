const OffsetLog = require('async-flumelog')
const bipf = require('bipf')
const { BLOCK_SIZE, newLogPath } = require('./defaults')

module.exports = function (dir, config, private) {
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

  // add automatic decrypt

  let originalGet = log.get
  log.get = function (seq, cb) {
    originalGet(seq, (err, res) => {
      if (err && err.code === 'flumelog:deleted') cb()
      else if (err) return cb(err)
      else cb(null, private.decrypt({ seq, value: res }).value)
    })
  }

  let originalStream = log.stream
  log.stream = function (opts) {
    let s = originalStream(opts)
    let originalPipe = s.pipe.bind(s)
    s.pipe = function (o) {
      let originalWrite = o.write
      o.write = (record) => originalWrite(private.decrypt(record))
      return originalPipe(o)
    }
    return s
  }

  return log
}
