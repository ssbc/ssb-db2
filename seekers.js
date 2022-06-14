// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const { allocAndEncode, seekKey2 } = require('bipf')

const BIPF_KEY = allocAndEncode('key')
const BIPF_AUTHOR = allocAndEncode('author')
const BIPF_CONTENT = allocAndEncode('content')
const BIPF_TYPE = allocAndEncode('type')
const BIPF_ROOT = allocAndEncode('root')
const BIPF_FORK = allocAndEncode('fork')
const BIPF_ABOUT = allocAndEncode('about')
const BIPF_BRANCH = allocAndEncode('branch')
const BIPF_VOTE = allocAndEncode('vote')
const BIPF_CONTACT = allocAndEncode('contact')
const BIPF_LINK = allocAndEncode('link')
const BIPF_META = allocAndEncode('meta')
const BIPF_PRIVATE = allocAndEncode('private')
const BIPF_ENCRYPTION_FORMAT = allocAndEncode('encryptionFormat')
const BIPF_CHANNEL = allocAndEncode('channel')
const BIPF_MENTIONS = allocAndEncode('mentions')

module.exports = {
  seekAuthor(buffer, start, pValue) {
    if (pValue < 0) return -1
    return seekKey2(buffer, pValue, BIPF_AUTHOR, 0)
  },

  seekType(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = seekKey2(buffer, pValue, BIPF_CONTENT, 0)
    if (pValueContent < 0) return -1
    return seekKey2(buffer, pValueContent, BIPF_TYPE, 0)
  },

  seekRoot(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = seekKey2(buffer, pValue, BIPF_CONTENT, 0)
    if (pValueContent < 0) return -1
    return seekKey2(buffer, pValueContent, BIPF_ROOT, 0)
  },

  seekFork(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = seekKey2(buffer, pValue, BIPF_CONTENT, 0)
    if (pValueContent < 0) return -1
    return seekKey2(buffer, pValueContent, BIPF_FORK, 0)
  },

  seekBranch(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = seekKey2(buffer, pValue, BIPF_CONTENT, 0)
    if (pValueContent < 0) return -1
    return seekKey2(buffer, pValueContent, BIPF_BRANCH, 0)
  },

  seekContent(buffer, start, pValue) {
    if (pValue < 0) return -1
    return seekKey2(buffer, pValue, BIPF_CONTENT, 0)
  },

  seekVoteLink(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = seekKey2(buffer, pValue, BIPF_CONTENT, 0)
    if (pValueContent < 0) return -1
    const pValueContentVote = seekKey2(buffer, pValueContent, BIPF_VOTE, 0)
    if (pValueContentVote < 0) return -1
    return seekKey2(buffer, pValueContentVote, BIPF_LINK, 0)
  },

  seekContact(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = seekKey2(buffer, pValue, BIPF_CONTENT, 0)
    if (pValueContent < 0) return -1
    return seekKey2(buffer, pValueContent, BIPF_CONTACT, 0)
  },

  seekMentions(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = seekKey2(buffer, pValue, BIPF_CONTENT, 0)
    if (pValueContent < 0) return -1
    return seekKey2(buffer, pValueContent, BIPF_MENTIONS, 0)
  },

  seekAbout(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = seekKey2(buffer, pValue, BIPF_CONTENT, 0)
    if (pValueContent < 0) return -1
    return seekKey2(buffer, pValueContent, BIPF_ABOUT, 0)
  },

  pluckLink(buffer, start) {
    return seekKey2(buffer, start, BIPF_LINK, 0)
  },

  seekMetaPrivate(buffer, start, pValue) {
    const pMeta = seekKey2(buffer, 0, BIPF_META, 0)
    if (pMeta < 0) return -1
    return seekKey2(buffer, pMeta, BIPF_PRIVATE, 0)
  },

  seekMetaEncryptionFormat(buffer, start, pValue) {
    const pMeta = seekKey2(buffer, 0, BIPF_META, 0)
    if (pMeta < 0) return -1
    return seekKey2(buffer, pMeta, BIPF_ENCRYPTION_FORMAT, 0)
  },

  seekMeta(buffer, start, pValue) {
    return seekKey2(buffer, 0, BIPF_META, 0)
  },

  seekChannel(buffer, start, pValue) {
    if (pValue < 0) return -1
    const pValueContent = seekKey2(buffer, pValue, BIPF_CONTENT, 0)
    if (pValueContent < 0) return -1
    return seekKey2(buffer, pValueContent, BIPF_CHANNEL, 0)
  },

  seekKey(buffer, start, pValue) {
    return seekKey2(buffer, 0, BIPF_KEY, 0)
  },
}
