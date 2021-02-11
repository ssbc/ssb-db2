const AboutSelfIndex = require('./indexes/about-self')

exports.init = function (sbot, config) {
  sbot.db.registerIndex(AboutSelfIndex)
}
