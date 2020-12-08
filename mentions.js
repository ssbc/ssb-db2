const MentionsIndex = require('./indexes/mentions')

exports.init = function (sbot, config) {
  sbot.db.registerIndex(MentionsIndex)
}
