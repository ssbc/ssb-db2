const EBTIndex = require('../indexes/ebt')
const { onceWhen } = require('../utils')

exports.init = function (sbot, config) {
  sbot.db.registerIndex(EBTIndex)
  if (!sbot.post) sbot.post = sbot.db.post
  sbot.getAtSequence = (key, cb) => {
    sbot.db.onDrain('ebt', () => {
      sbot.db.getIndex('ebt').getMessageFromAuthorSequence(key, cb)
    })
  }
  sbot.add = sbot.db.add
  sbot.getVectorClock = function (cb) {
    onceWhen(
      sbot.db2migrate && sbot.db2migrate.synchronized,
      (isSynced) => isSynced,
      () => {
        sbot.db.onDrain('base', () => {
          sbot.db.getAllLatest((err, latest) => {
            if (err) return cb(err)

            const clock = {}
            for (const [authorId, { sequence }] of latest) {
              clock[authorId] = sequence
            }

            cb(null, clock)
          })
        })
      }
    )
  }
}
