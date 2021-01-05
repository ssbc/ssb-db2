const OffsetLog = require('async-append-only-log')
const bipf = require('bipf')
const { BLOCK_SIZE, newLogPath } = require('./defaults')

module.exports = function (dir, config, privateIndex) {
  config = config || {}

  const log = OffsetLog(newLogPath(dir), {
    blockSize: BLOCK_SIZE,
    validateRecord: (d) => {
      try {
        bipf.decode(d, 0)
        return true
      } catch (ex) {
        return false
      }
    },
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

  const originalGet = log.get
  log.get = function (offset, cb) {
    originalGet(offset, (err, buffer) => {
      if (err) return cb(err)
      else {
        const record = { offset, value: buffer }
        cb(null, privateIndex.decrypt(record, false).value)
      }
    })
  }

  const originalStream = log.stream
  log.stream = function (opts) {
    const s = originalStream(opts)
    const originalPipe = s.pipe.bind(s)
    s.pipe = function pipe(o) {
      let originalWrite = o.write
      o.write = (record) => originalWrite(privateIndex.decrypt(record, true))
      return originalPipe(o)
    }
    return s
  }

  return log
}
