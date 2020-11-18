const DB = require('./db')

exports.name = 'db'

exports.init = function (sbot, config) {
  const db = DB.init(config.path, config)
  sbot.close.hook(function (fn, args) {
    db.close()
    return fn.apply(this, args)
  })
  return db
}
