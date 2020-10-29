const db = require('./db')

exports.name = 'db'

exports.init = function (sbot, config) {
  return db.init(config.path, config)
}
