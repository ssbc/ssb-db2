const { deferred } = require('jitdb/operators')

module.exports = function mentions(key) {
  return deferred((meta, cb) => {
    meta.db2.onDrain('fullMentions', () => {
      meta.db2.getIndex('fullMentions').getMessagesByMention(key, meta.live, cb)
    })
  })
}
