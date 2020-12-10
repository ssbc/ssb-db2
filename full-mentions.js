const FullMentionsIndex = require('./indexes/full-mentions')

exports.init = function (sbot, config) {
  sbot.db.registerIndex(FullMentionsIndex)
}
