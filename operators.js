const jitdbOperators = require('jitdb/operators')
const seekers = require('./seekers')

// override query() from jitdb to implicitly call fromDB()
function query(first, ...rest) {
  if (!first.meta && first.jitdb) {
    return jitdbOperators.query(jitdbOperators.fromDB(first.jitdb), ...rest)
  } else {
    return jitdbOperators.query(first, ...rest)
  }
}

function toBuffer(value) {
  return Buffer.isBuffer(value) ? value : Buffer.from(value)
}

function type(value) {
  return {
    type: 'EQUAL',
    data: {
      seek: seekers.seekType,
      value: toBuffer(value),
      indexType: 'type',
    },
  };
}

function author(value) {
  return {
    type: 'EQUAL',
    data: {
      seek: seekers.seekAuthor,
      value: toBuffer(value),
      indexType: 'author',
    },
  };
}

function channel(value) {
  return {
    type: 'EQUAL',
    data: {
      seek: seekers.seekChannel,
      value: toBuffer(value),
      indexType: 'channel',
    },
  };
}

function isRoot() {
  return {
    type: 'EQUAL',
    data: {
      seek: seekers.seekRoot,
      value: undefined,
      indexType: 'root',
    },
  };
}

const bTrue = Buffer.from('true')
function isPrivate() {
  return {
    type: 'EQUAL',
    data: {
      seek: seekers.seekPrivate,
      value: bTrue,
      indexType: 'private',
    },
  };
}

module.exports = Object.assign({}, jitdbOperators, {
  query,
  type,
  author,
  channel,
  isRoot,
  isPrivate
})