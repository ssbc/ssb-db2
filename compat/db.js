// exports.name is blank to merge into global namespace

exports.manifest = {
  publish: 'async',
  whoami: 'sync',
}

exports.init = function (sbot, config) {
  sbot.publish = sbot.db.publish
  sbot.whoami = () => ({ id: sbot.id })
}
