const DB = require('./db')

exports.name = 'db'

exports.version = DB.version

exports.manifest = DB.manifest

exports.init = function (sbot, config) {
  const db = DB.init(sbot, config.path, config)
  sbot.close.hook(function (fn, args) {
    db.close(() => {
      fn.apply(this, args)
    })
  })
  return db
}
