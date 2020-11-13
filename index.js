const DB = require('./db')

exports.name = 'db'

exports.init = function (sbot, config) {
  return DB.init(config.path, config)
}
