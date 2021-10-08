// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const { seekKey } = require('bipf')

const B_KEY = Buffer.from('key')
const B_VALUE = Buffer.from('value')
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
  seekAuthor: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, B_VALUE)
    if (p < 0) return
    return seekKey(buffer, p, B_AUTHOR)
  },

  seekType: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, B_VALUE)
    if (p < 0) return
    p = seekKey(buffer, p, B_CONTENT)
    if (p < 0) return
    return seekKey(buffer, p, B_TYPE)
  },

  seekRoot: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, B_VALUE)
    if (p < 0) return
    p = seekKey(buffer, p, B_CONTENT)
    if (p < 0) return
    return seekKey(buffer, p, B_ROOT)
  },

  seekFork: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, B_VALUE)
    if (p < 0) return
    p = seekKey(buffer, p, B_CONTENT)
    if (p < 0) return
    return seekKey(buffer, p, B_FORK)
  },

  seekBranch: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, B_VALUE)
    if (p < 0) return
    p = seekKey(buffer, p, B_CONTENT)
    if (p < 0) return
    return seekKey(buffer, p, B_BRANCH)
  },

  seekVoteLink: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, B_VALUE)
    if (p < 0) return
    p = seekKey(buffer, p, B_CONTENT)
    if (p < 0) return
    p = seekKey(buffer, p, B_VOTE)
    if (p < 0) return
    return seekKey(buffer, p, B_LINK)
  },

  seekContact: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, B_VALUE)
    if (p < 0) return
    p = seekKey(buffer, p, B_CONTENT)
    if (p < 0) return
    return seekKey(buffer, p, B_CONTACT)
  },

  seekMentions: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, B_VALUE)
    if (p < 0) return
    p = seekKey(buffer, p, B_CONTENT)
    if (p < 0) return
    return seekKey(buffer, p, B_MENTIONS)
  },

  seekAbout: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, B_VALUE)
    if (p < 0) return
    p = seekKey(buffer, p, B_CONTENT)
    if (p < 0) return
    return seekKey(buffer, p, B_ABOUT)
  },

  pluckLink: function (buffer, start) {
    let p = start
    return seekKey(buffer, p, B_LINK)
  },

  seekPrivate: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, B_META)
    if (p < 0) return
    return seekKey(buffer, p, B_PRIVATE)
  },

  seekMeta: function (buffer) {
    let p = 0 // note you pass in p!
    return seekKey(buffer, p, B_META)
  },

  seekChannel: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, B_VALUE)
    if (p < 0) return
    p = seekKey(buffer, p, B_CONTENT)
    if (p < 0) return
    return seekKey(buffer, p, B_CHANNEL)
  },

  seekKey: function (buffer) {
    var p = 0 // note you pass in p!
    return seekKey(buffer, p, B_KEY)
  },
}
