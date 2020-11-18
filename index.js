const DB = require('./db')

exports.name = 'db'

exports.init = function (sbot, config) {
  const db = DB.init(sbot, config.path, config)
  sbot.close.hook(function (fn, args) {
    db.close()
    return fn.apply(this, args)
  })
  return db
}
