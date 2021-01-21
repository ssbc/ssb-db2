const OffsetLog = require('async-append-only-log')
const bipf = require('bipf')
const TooHot = require('too-hot')
const { BLOCK_SIZE, newLogPath } = require('./defaults')

module.exports = function (dir, config, privateIndex) {
  config = config || {}
  config.db2 = config.db2 || {}

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

  // monkey-patch log.get to decrypt the msg
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

  // monkey-patch log.stream to temporarily pause when the CPU is too busy,
  // and to decrypt the msg
  const originalStream = log.stream
  log.stream = function (opts) {
    const tooHot = config.db2.maxCpu
      ? TooHot({ ceiling: config.db2.maxCpu, maxPause: 1000 })
      : () => false
    const s = originalStream(opts)
    const originalPipe = s.pipe.bind(s)
    s.pipe = function pipe(o) {
      let originalWrite = o.write
      o.write = (record) => {
        const hot = tooHot()
        if (hot && !s.sink.paused) {
          s.sink.paused = true
          hot.then(() => {
            s.sink.paused = false
            s.resume()
            originalWrite(privateIndex.decrypt(record, true))
          })
        } else {
          originalWrite(privateIndex.decrypt(record, true))
        }
      }
      return originalPipe(o)
    }
    return s
  }

  return log
}
