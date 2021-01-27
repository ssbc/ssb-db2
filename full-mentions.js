const FullMentionsIndex = require('./indexes/full-mentions')
const fullMentions = require('./operators/full-mentions')

exports.init = function (sbot, config) {
  sbot.db.registerIndex(FullMentionsIndex)
  sbot.db.operators.fullMentions = fullMentions
}
