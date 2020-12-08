const jitdbOperators = require('jitdb/operators')
const {
  seekKey,
  seekType,
  seekAuthor,
  seekChannel,
  seekRoot,
  seekPrivate,
} = require('../seekers')
const { equal } = jitdbOperators

function key(value) {
  return equal(seekKey, value, { indexType: 'key', prefix: 32 })
}

function type(value) {
  return equal(seekType, value, { indexType: 'value_content_type' })
}

function author(value) {
  return equal(seekAuthor, value, { indexType: 'value_author' })
}

function channel(value) {
  return equal(seekChannel, value, { indexType: 'value_content_channel' })
}

function isRoot() {
  return equal(seekRoot, null, {
    prefix: 32,
    indexType: 'value_content_root',
  })
}

let bTrue = Buffer.alloc(1)
bTrue[0] = 1
function isPrivate() {
  return equal(seekPrivate, bTrue, { indexType: 'meta_private' })
}

module.exports = Object.assign({}, jitdbOperators, {
  type,
  author,
  channel,
  key,
  isRoot,
  isPrivate,
})
