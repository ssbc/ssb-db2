const dbPlugin = require('./db')
const migratePlugin = require('./migrate')

module.exports = [dbPlugin, migratePlugin]
