const OffsetLog = require('async-flumelog')
const bipf = require('bipf')
const { BLOCK_SIZE, newLogPath } = require('./defaults')

module.exports = function (dir, config, private) {
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

  let originalGet = log.get
  log.get = function (offset, cb) {
    originalGet(offset, (err, buffer) => {
      if (err) return cb(err)
      else {
        // "seq" in flumedb is an abstract num, here it actually means "offset"
        const record = { seq: offset, value: buffer }
        cb(null, private.decrypt(record, false).value)
      }
    })
  }

  let originalStream = log.stream
  log.stream = function (opts) {
    let s = originalStream(opts)
    let originalPipe = s.pipe.bind(s)
    s.pipe = function (o) {
      let originalWrite = o.write
      o.write = (record) => originalWrite(private.decrypt(record, true))
      return originalPipe(o)
    }
    return s
  }

  return log
}
