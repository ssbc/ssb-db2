const DB = require('./db')

exports.name = 'db'

exports.init = function (sbot, config) {
  const db = DB.init(config.path, config)

  // backwards-compatibility
  sbot.publish = db.publish

  // EBT
  sbot.post = db.post
  sbot.getAtSequence = db.getMessageFromAuthorSequence
  sbot.getVectorClock = function(cb) {
    db.getAllLatest((err, last) => {
      if (err) return cb(err)

      var clock = {}
      for (var k in last) {
        clock[k] = last[k].sequence
      }

      cb(null, clock)
    })
  }

  return db
}
