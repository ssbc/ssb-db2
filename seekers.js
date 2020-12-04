const { seekKey } = require('bipf')

const bKey = Buffer.from('key')
const bValue = Buffer.from('value')
const bAuthor = Buffer.from('author')
const bContent = Buffer.from('content')
const bType = Buffer.from('type')
const bRoot = Buffer.from('root')
const bMeta = Buffer.from('meta')
const bPrivate = Buffer.from('private')
const bChannel = Buffer.from('channel')

module.exports = {
  seekAuthor: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, bValue)
    if (!~p) return
    return seekKey(buffer, p, bAuthor)
  },

  seekType: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, bValue)
    if (!~p) return
    p = seekKey(buffer, p, bContent)
    if (!~p) return
    return seekKey(buffer, p, bType)
  },

  seekRoot: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, bValue)
    if (!~p) return
    p = seekKey(buffer, p, bContent)
    if (!~p) return
    return seekKey(buffer, p, bRoot)
  },

  seekPrivate: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, bMeta)
    if (!~p) return
    return seekKey(buffer, p, bPrivate)
  },

  seekChannel: function (buffer) {
    let p = 0 // note you pass in p!
    p = seekKey(buffer, p, bValue)
    if (!~p) return
    p = seekKey(buffer, p, bContent)
    if (!~p) return
    return seekKey(buffer, p, bChannel)
  },

  seekKey: function (buffer) {
    var p = 0 // note you pass in p!
    return seekKey(buffer, p, bKey)
  },
}
