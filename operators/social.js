const {deferred} = require('jitdb/operators')

function hasRoot(msgKey) {
  return deferred((meta, cb) => {
    meta.db2.indexes.social.getMessagesByRoot(msgKey, cb)
  })
}

function votesFor(msgKey) {
  return deferred((meta, cb) => {
    meta.db2.indexes.social.getMessagesByVoteLink(msgKey, cb)
  })
}

function mentions(key) {
  return deferred((meta, cb) => {
    meta.db2.indexes.social.getMessagesByMention(key, cb)
  })
}

module.exports = {
  hasRoot,
  votesFor,
  mentions,
}