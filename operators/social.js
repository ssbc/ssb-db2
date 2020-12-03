const { deferred } = require('jitdb/operators')

function hasRoot(msgKey) {
  return deferred((meta, cb) => {
    meta.db2.getIndexes().social.getMessagesByRoot(msgKey, meta.live, cb)
  })
}

function votesFor(msgKey) {
  return deferred((meta, cb) => {
    meta.db2.getIndexes().social.getMessagesByVoteLink(msgKey, meta.live, cb)
  })
}

function mentions(key) {
  return deferred((meta, cb) => {
    meta.db2.getIndexes().social.getMessagesByMention(key, meta.live, cb)
  })
}

module.exports = {
  hasRoot,
  votesFor,
  mentions,
}
