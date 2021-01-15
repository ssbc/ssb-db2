const { seekKey } = require('bipf')

const bKey = Buffer.from('key')
const bValue = Buffer.from('value')
const bAuthor = Buffer.from('author')
const bContent = Buffer.from('content')
const bType = Buffer.from('type')
const bRoot = Buffer.from('root')
const bFork = Buffer.from('fork')
const bVote = Buffer.from('vote')
const bContact = Buffer.from('contact')
const bLink = Buffer.from('link')
const bMeta = Buffer.from('meta')
const bPrivate = Buffer.from('private')
const bChannel = Buffer.from('channel')
const bMentions = Buffer.from('mentions')

module.exports = {
  seekAuthor: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, bValue)
    if (p < 0) return
    return seekKey(buffer, p, bAuthor)
  },

  seekType: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, bValue)
    if (p < 0) return
    p = seekKey(buffer, p, bContent)
    if (p < 0) return
    return seekKey(buffer, p, bType)
  },

  seekRoot: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, bValue)
    if (p < 0) return
    p = seekKey(buffer, p, bContent)
    if (p < 0) return
    return seekKey(buffer, p, bRoot)
  },

  seekFork: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, bValue)
    if (p < 0) return
    p = seekKey(buffer, p, bContent)
    if (p < 0) return
    return seekKey(buffer, p, bFork)
  },

  seekVoteLink: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, bValue)
    if (p < 0) return
    p = seekKey(buffer, p, bContent)
    if (p < 0) return
    p = seekKey(buffer, p, bVote)
    if (p < 0) return
    return seekKey(buffer, p, bLink)
  },

  seekContact: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, bValue)
    if (p < 0) return
    p = seekKey(buffer, p, bContent)
    if (p < 0) return
    return seekKey(buffer, p, bContact)
  },

  seekMentions: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, bValue)
    if (p < 0) return
    p = seekKey(buffer, p, bContent)
    if (p < 0) return
    return seekKey(buffer, p, bMentions)
  },

  pluckLink: function (buffer, start) {
    let p = start
    return seekKey(buffer, p, bLink)
  },

  seekPrivate: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, bMeta)
    if (p < 0) return
    return seekKey(buffer, p, bPrivate)
  },

  seekMeta: function (buffer) {
    let p = 0 // note you pass in p!
    return seekKey(buffer, p, bMeta)
  },

  seekChannel: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, bValue)
    if (p < 0) return
    p = seekKey(buffer, p, bContent)
    if (p < 0) return
    return seekKey(buffer, p, bChannel)
  },

  seekKey: function (buffer) {
    var p = 0 // note you pass in p!
    return seekKey(buffer, p, bKey)
  },
}
