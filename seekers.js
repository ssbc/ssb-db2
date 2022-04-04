// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const { seekKey, seekKeyCached } = require('bipf')

const B_KEY = Buffer.from('key')
const B_ROOT = Buffer.from('root')
const B_FORK = Buffer.from('fork')
const B_ABOUT = Buffer.from('about')
const B_BRANCH = Buffer.from('branch')
const B_VOTE = Buffer.from('vote')
const B_CONTACT = Buffer.from('contact')
const B_LINK = Buffer.from('link')
const B_PRIVATE = Buffer.from('private')
const B_CHANNEL = Buffer.from('channel')

module.exports = {
  seekAuthor(buffer) {
    const pValue = seekKeyCached(buffer, 0, 'value')
    if (pValue < 0) return
    return seekKeyCached(buffer, pValue, 'author')
  },

  seekType(buffer) {
    const pValue = seekKeyCached(buffer, 0, 'value')
    if (pValue < 0) return
    const pValueContent = seekKeyCached(buffer, pValue, 'content')
    if (pValueContent < 0) return
    return seekKeyCached(buffer, pValueContent, 'type')
  },

  seekRoot(buffer) {
    const pValue = seekKeyCached(buffer, 0, 'value')
    if (pValue < 0) return
    const pValueContent = seekKeyCached(buffer, pValue, 'content')
    if (pValueContent < 0) return
    return seekKey(buffer, pValueContent, B_ROOT)
  },

  seekFork(buffer) {
    const pValue = seekKeyCached(buffer, 0, 'value')
    if (pValue < 0) return
    const pValueContent = seekKeyCached(buffer, pValue, 'content')
    if (pValueContent < 0) return
    return seekKey(buffer, pValueContent, B_FORK)
  },

  seekBranch(buffer) {
    const pValue = seekKeyCached(buffer, 0, 'value')
    if (pValue < 0) return
    const pValueContent = seekKeyCached(buffer, pValue, 'content')
    return seekKey(buffer, pValueContent, B_BRANCH)
  },

  seekVoteLink(buffer) {
    const pValue = seekKeyCached(buffer, 0, 'value')
    if (pValue < 0) return
    const pValueContent = seekKeyCached(buffer, pValue, 'content')
    if (pValueContent < 0) return
    const pValueContentVote = seekKey(buffer, pValueContent, B_VOTE)
    if (pValueContentVote < 0) return
    return seekKey(buffer, pValueContentVote, B_LINK)
  },

  seekContact(buffer) {
    const pValue = seekKeyCached(buffer, 0, 'value')
    if (pValue < 0) return
    const pValueContent = seekKeyCached(buffer, pValue, 'content')
    return seekKey(buffer, pValueContent, B_CONTACT)
  },

  seekMentions(buffer) {
    const pValue = seekKeyCached(buffer, 0, 'value')
    if (pValue < 0) return
    const pValueContent = seekKeyCached(buffer, pValue, 'content')
    if (pValueContent < 0) return
    return seekKeyCached(buffer, pValueContent, 'mentions')
  },

  seekAbout(buffer) {
    const pValue = seekKeyCached(buffer, 0, 'value')
    if (pValue < 0) return
    const pValueContent = seekKeyCached(buffer, pValue, 'content')
    if (pValueContent < 0) return
    return seekKey(buffer, pValueContent, B_ABOUT)
  },

  pluckLink(buffer, start) {
    return seekKey(buffer, start, B_LINK)
  },

  seekPrivate(buffer) {
    const pMeta = seekKeyCached(buffer, 0, 'meta')
    if (pMeta < 0) return
    return seekKey(buffer, pMeta, B_PRIVATE)
  },

  seekMeta(buffer) {
    return seekKeyCached(buffer, 0, 'meta')
  },

  seekChannel(buffer) {
    const pValue = seekKeyCached(buffer, 0, 'value')
    if (pValue < 0) return
    const pValueContent = seekKeyCached(buffer, pValue, 'content')
    if (pValueContent < 0) return
    return seekKey(buffer, pValueContent, B_CHANNEL)
  },

  seekKey(buffer) {
    return seekKeyCached(buffer, 0, 'key')
  },
}
