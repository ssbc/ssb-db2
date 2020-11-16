var OffsetLog = require('async-flumelog')
var bipf = require('bipf')
var path = require('path')

module.exports = function (dir, config) {
  config = config || {}

  var log = OffsetLog(
    path.join(dir, 'db2', 'log.bipf'),
    { blockSize:1024*64 }
  )

  log.add = function (id, msg, cb) {
    var data = {
      key: id,
      value: msg,
      timestamp: Date.now()
    }
    var b = Buffer.alloc(bipf.encodingLength(data))
    bipf.encode(data, b, 0)
    log.append(b, function (err) {
      if (err) cb(err)
      else cb(null, data)
    })
  }

  return log
}
