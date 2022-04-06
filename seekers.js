// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const { seekKey } = require('bipf')

const B_KEY = Buffer.from('key')
const B_AUTHOR = Buffer.from('author')
const B_CONTENT = Buffer.from('content')
const B_TYPE = Buffer.from('type')
const B_ROOT = Buffer.from('root')
const B_FORK = Buffer.from('fork')
const B_ABOUT = Buffer.from('about')
const B_BRANCH = Buffer.from('branch')
const B_VOTE = Buffer.from('vote')
const B_CONTACT = Buffer.from('contact')
const B_LINK = Buffer.from('link')
const B_META = Buffer.from('meta')
const B_PRIVATE = Buffer.from('private')
const B_CHANNEL = Buffer.from('channel')
const B_MENTIONS = Buffer.from('mentions')

module.exports = {
  seekAuthor(buffer, start, pValue) {
    if (pValue < 0) return -1
    return seekKey(buffer, pValue, B_AUTHOR)
  },

  seekType(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = seekKey(buffer, pValue, B_CONTENT)
    if (pValueContent < 0) return -1
    return seekKey(buffer, pValueContent, B_TYPE)
  },

  seekRoot(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = seekKey(buffer, pValue, B_CONTENT)
    if (pValueContent < 0) return -1
    return seekKey(buffer, pValueContent, B_ROOT)
  },

  seekFork(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = seekKey(buffer, pValue, B_CONTENT)
    if (pValueContent < 0) return -1
    return seekKey(buffer, pValueContent, B_FORK)
  },

  seekBranch(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = seekKey(buffer, pValue, B_CONTENT)
    if (pValueContent < 0) return -1
    return seekKey(buffer, pValueContent, B_BRANCH)
  },

  seekVoteLink(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = seekKey(buffer, pValue, B_CONTENT)
    if (pValueContent < 0) return -1
    const pValueContentVote = seekKey(buffer, pValueContent, B_VOTE)
    if (pValueContentVote < 0) return -1
    return seekKey(buffer, pValueContentVote, B_LINK)
  },

  seekContact(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = seekKey(buffer, pValue, B_CONTENT)
    if (pValueContent < 0) return -1
    return seekKey(buffer, pValueContent, B_CONTACT)
  },

  seekMentions(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = seekKey(buffer, pValue, B_CONTENT)
    if (pValueContent < 0) return -1
    return seekKey(buffer, pValueContent, B_MENTIONS)
  },

  seekAbout(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = seekKey(buffer, pValue, B_CONTENT)
    if (pValueContent < 0) return -1
    return seekKey(buffer, pValueContent, B_ABOUT)
  },

  pluckLink(buffer, start) {
    return seekKey(buffer, start, B_LINK)
  },

  seekPrivate(buffer, start, pValue) {
    const pMeta = seekKey(buffer, 0, B_META)
    if (pMeta < 0) return -1
    return seekKey(buffer, pMeta, B_PRIVATE)
  },

  seekMeta(buffer, start, pValue) {
    return seekKey(buffer, 0, B_META)
  },

  seekChannel(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = seekKey(buffer, pValue, B_CONTENT)
    if (pValueContent < 0) return -1
    return seekKey(buffer, pValueContent, B_CHANNEL)
  },

  seekKey(buffer, start, pValue) {
    return seekKey(buffer, 0, B_KEY)
  },
}
