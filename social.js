const SocialIndex = require('./indexes/social')

exports.init = function (sbot, config) {
  sbot.db.registerIndex(SocialIndex)
}
