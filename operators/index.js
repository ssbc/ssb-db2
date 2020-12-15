const jitdbOperators = require('jitdb/operators')
const {
  seekKey,
  seekType,
  seekAuthor,
  seekChannel,
  seekRoot,
  seekPrivate,
  seekVoteLink,
  seekMentions,
  pluckLink,
  seekContact,
} = require('../seekers')
const { and, equal, includes } = jitdbOperators

function key(value) {
  return equal(seekKey, value, {
    prefix: 32,
    indexType: 'key',
  })
}

function type(value) {
  return equal(seekType, value, {
    indexType: 'value_content_type',
  })
}

function author(value) {
  return equal(seekAuthor, value, {
    indexType: 'value_author',
  })
}

function channel(value) {
  return equal(seekChannel, value, {
    indexType: 'value_content_channel',
  })
}

function votesFor(msgKey) {
  return and(
    type('vote'),
    equal(seekVoteLink, msgKey, {
      prefix: 32,
      indexType: 'value_content_vote_link',
    })
  )()
}

function contact(feedId) {
  return and(
    type('contact'),
    equal(seekContact, feedId, {
      prefix: 32,
      indexType: 'value_content_contact',
    })
  )()
}

function mentions(key) {
  return includes(seekMentions, key, {
    pluck: pluckLink,
    indexType: 'value_content_mentions_link',
  })
}

function hasRoot(msgKey) {
  return equal(seekRoot, msgKey, {
    prefix: 32,
    indexType: 'value_content_root',
  })
}

function isRoot() {
  return equal(seekRoot, null, {
    indexType: 'value_content_root',
  })
}

const bTrue = Buffer.alloc(1, 1)
function isPrivate() {
  return equal(seekPrivate, bTrue, { indexType: 'meta_private' })
}

module.exports = Object.assign({}, jitdbOperators, {
  type,
  author,
  channel,
  key,
  votesFor,
  contact,
  mentions,
  hasRoot,
  isRoot,
  isPrivate,
})
