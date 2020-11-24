const path = require('path')

exports.BLOCK_SIZE = 64 * 1024
exports.oldLogPath = (dir) => path.join(dir, 'flume', 'log.offset')
exports.newLogPath = (dir) => path.join(dir, 'db2', 'log.bipf')
exports.indexesPath = (dir) => path.join(dir, 'db2', 'indexes')
