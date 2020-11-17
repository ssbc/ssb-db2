exports.init = function (sbot, config) {
  sbot.post = sbot.db.post
  sbot.getAtSequence = sbot.db.getMessageFromAuthorSequence
  sbot.getVectorClock = function (cb) {
    sbot.db.getAllLatest((err, last) => {
      if (err) return cb(err)

      const clock = {}
      for (const k in last) {
        clock[k] = last[k].sequence
      }

      cb(null, clock)
    })
  }
}
