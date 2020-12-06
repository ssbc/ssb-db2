const jitdbOperators = require('jitdb/operators')
const seekers = require('../seekers')
const { equal } = jitdbOperators

function key(value) {
  return equal(seekers.seekKey, value, { indexType: 'key', prefix: 32 })
}

function type(value) {
  return equal(seekers.seekType, value, { indexType: 'type' })
}

function author(value) {
  return equal(seekers.seekAuthor, value, { indexType: 'author' })
}

function channel(value) {
  return equal(seekers.seekChannel, value, { indexType: 'channel' })
}

function isRoot() {
  return equal(seekers.seekRoot, undefined, { indexType: 'root' })
}

let bTrue = Buffer.alloc(1)
bTrue[0] = 1
function isPrivate() {
  return equal(seekers.seekPrivate, bTrue, { indexType: 'private' })
}

module.exports = Object.assign({}, jitdbOperators, {
  type,
  author,
  channel,
  key,
  isRoot,
  isPrivate,
})
