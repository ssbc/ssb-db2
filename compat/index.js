const db = require('./db')
const ebt = require('./ebt')
const hist = require('./history-stream')
const logstream = require('./log-stream')

exports.init = function (sbot, config) {
  db.init(sbot, config)
  ebt.init(sbot, config)
  hist.init(sbot, config)
  logstream.init(sbot, config)
}
