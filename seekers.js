const bipf = require('bipf')

const bValue = Buffer.from('value')
const bAuthor = Buffer.from('author')
const bContent = Buffer.from('content')
const bType = Buffer.from('type')
const bRoot = Buffer.from('root')
const bMeta = Buffer.from('meta')
const bPrivate = Buffer.from('private')
const bChannel = Buffer.from('channel')

module.exports = {
  seekAuthor: function(buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, bValue)
    if (!~p) return
    return bipf.seekKey(buffer, p, bAuthor)
  },

  seekType: function(buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, bValue)
    if (!~p) return
    p = bipf.seekKey(buffer, p, bContent)
    if (!~p) return
    return bipf.seekKey(buffer, p, bType)
  },

  seekRoot: function(buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, bValue)
    if (!~p) return
    p = bipf.seekKey(buffer, p, bContent)
    if (!~p) return
    return bipf.seekKey(buffer, p, bRoot)
  },

  seekPrivate: function(buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, bValue)
    if (!~p) return
    p = bipf.seekKey(buffer, p, bMeta)
    if (!~p) return
    return bipf.seekKey(buffer, p, bPrivate)
  },

  seekChannel: function(buffer) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(buffer, p, bValue)
    if (!~p) return
    p = bipf.seekKey(buffer, p, bContent)
    if (!~p) return
    return bipf.seekKey(buffer, p, bChannel)
  }
}
