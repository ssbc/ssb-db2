// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const SSBURI = require('ssb-uri2')
const jitdbOperators = require('jitdb/operators')
const {
  seekType,
  seekAuthor,
  seekChannel,
  seekRoot,
  seekFork,
  seekPrivate,
  seekMeta,
  seekVoteLink,
  seekMentions,
  pluckLink,
  seekContact,
  seekBranch,
  seekAbout,
} = require('../seekers')
const { and, equal, predicate, includes, deferred } = jitdbOperators

function key(msgId) {
  return deferred((meta, cb) => {
    meta.db.onDrain('keys', () => {
      meta.db.getIndex('keys').getMsgByKey(msgId, cb)
    })
  })
}

function type(value, opts = { dedicated: true }) {
  if (opts && opts.dedicated) {
    return equal(seekType, value, {
      indexType: 'value_content_type',
    })
  } else {
    return equal(seekType, value, {
      prefix: 32,
      indexType: 'value_content_type',
    })
  }
}

// We don't need the author "prefix" to be an actual prefix, it can just be any
// predefined positions in the "information" part of the author ID.
//
// WARNING: when updating this, be extra careful that the resulting number isn't
// larger than the smallest ID's length. E.g. classic feed IDs are 53 characters
// long, and the base64 part ends at character 44, so AUTHOR_PREFIX_OFFSET must
// be smaller than 40 (i.e. 44 - 4).
const AUTHOR_PREFIX_OFFSET = Math.max(
  '@'.length,
  'ssb:feed/bendybutt-v1/'.length
)

function author(value, opts = { dedicated: false }) {
  if (opts && opts.dedicated) {
    return equal(seekAuthor, value, {
      indexType: 'value_author',
    })
  } else {
    return equal(seekAuthor, value, {
      prefix: 32,
      prefixOffset: AUTHOR_PREFIX_OFFSET,
      indexType: 'value_author',
      version: 3,
    })
  }
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
      prefixOffset: 1,
      useMap: true,
      indexType: 'value_content_vote_link',
    })
  )
}

function contact(feedId) {
  return and(
    type('contact'),
    equal(seekContact, feedId, {
      prefix: 32,
      prefixOffset: 1,
      useMap: true,
      indexType: 'value_content_contact',
    })
  )
}

function mentions(key) {
  return includes(seekMentions, key, {
    pluck: pluckLink,
    indexType: 'value_content_mentions_link',
  })
}

function about(feedId) {
  return and(
    type('about'),
    equal(seekAbout, feedId, {
      prefix: 32,
      prefixOffset: 1,
      useMap: true,
      indexType: 'value_content_about',
    })
  )
}

function hasRoot(msgKey) {
  return equal(seekRoot, msgKey, {
    prefix: 32,
    prefixOffset: 1,
    useMap: true,
    indexType: 'value_content_root',
  })
}

function hasFork(msgKey) {
  return equal(seekFork, msgKey, {
    prefix: 32,
    prefixOffset: 1,
    useMap: true,
    indexType: 'value_content_fork',
  })
}

function hasBranch(msgKey) {
  return equal(seekBranch, msgKey, {
    prefix: 32,
    prefixOffset: 1,
    useMap: true,
    indexType: 'value_content_branch',
  })
}

function authorIsBendyButtV1() {
  return predicate(seekAuthor, SSBURI.isBendyButtV1FeedSSBURI, {
    indexType: 'value_author',
    name: 'bendybutt-v1',
  })
}

function isRoot() {
  return equal(seekRoot, undefined, {
    indexType: 'value_content_root',
  })
}

function isPrivate() {
  return equal(seekPrivate, true, { indexType: 'meta_private' })
}

function isPublic() {
  return equal(seekMeta, undefined, { indexType: 'meta' })
}

module.exports = Object.assign({}, jitdbOperators, {
  type,
  author,
  channel,
  key,
  votesFor,
  contact,
  mentions,
  about,
  hasRoot,
  hasFork,
  hasBranch,
  authorIsBendyButtV1,
  isRoot,
  isPrivate,
  isPublic,
})
