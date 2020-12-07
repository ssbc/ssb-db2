const { deferred, equal } = require('jitdb/operators')
const seekers = require('../seekers')

function hasRoot(msgKey) {
  return equal(seekers.seekRoot, msgKey, {
    prefix: 32,
    indexType: 'value_content_root',
  })
}

function votesFor(msgKey) {
  return equal(seekers.seekVoteLink, msgKey, {
    prefix: 32,
    indexType: 'value_content_vote_link',
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
