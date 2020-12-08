const { deferred } = require('jitdb/operators')

module.exports = function mentions(key) {
  return deferred((meta, cb) => {
    meta.db2.getIndexes().mentions.getMessagesByMention(key, meta.live, cb)
  })
}
