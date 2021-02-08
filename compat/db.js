exports.init = function (sbot, config) {
  sbot.publish = sbot.db.publish
  sbot.whoami = () => ({ id: sbot.id })
}
