const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const {
  or,
  and,
  type,
  author,
  key,
  votesFor,
  hasRoot,
  isPublic,
  isRoot,
  startFrom,
  paginate,
  descending,
  toCallback,
} = require('./operators')

const pull = require('pull-stream')
const path = require('path')

const sbot = SecretStack({ caps })
  .use(require('./'))
  .call(null, {
    path: './ssb',
    keys: require('ssb-keys').loadOrCreateSync(path.join('ssb', 'secret')),
  })

// value.content.about = ...

var run = 0

let queries = {
  'key initial': [
    and(key('%HZVnEzm0NgoSVfG0Hx4gMFbMMHhFvhJsG2zK/pijYII=.sha256')),
  ],
  'key 2': [and(key('%3HZVnEzm0NgoSVfG0Hx4gMFbMMHhFvhJsG2zK/pijYII=.sha256'))],
  'key again': [
    and(key('%HZVnEzm0NgoSVfG0Hx4gMFbMMHhFvhJsG2zK/pijYII=.sha256')),
  ],
  'latest root posts': [
    and(type('post'), isRoot(), isPublic()),
    startFrom(0),
    paginate(25),
    descending(),
  ],
  'latest posts': [
    and(type('post'), isPublic()),
    startFrom(0),
    paginate(25),
    descending(),
  ],
  'votes initial': [
    and(votesFor('%HZVnEzm0NgoSVfG0Hx4gMFbMMHhFvhJsG2zK/pijYII=.sha256')),
  ],
  'votes 2': [
    and(votesFor('%HZVnEzm0NgoSVfG0Hx4gMFbMMHhFvhJsG2zK/pijYII=.sha256')),
  ],
  'votes again': [
    and(votesFor('%HZVnEzm0NgoSVfG0Hx4gMFbMMHhFvhJsG2zK/pijYII=.sha256')),
  ],
  hasRoot: [
    and(hasRoot('%HZVnEzm0NgoSVfG0Hx4gMFbMMHhFvhJsG2zK/pijYII=.sha256')),
  ],
  'hasRoot again': [
    and(hasRoot('%HZVnEzm0NgoSVfG0Hx4gMFbMMHhFvhJsG2zK/pijYII=.sha256')),
  ],
  'author posts': [
    and(
      type('post'),
      author('@6CAxOI3f+LUOVrbAl0IemqiS7ATpQvr9Mdw9LC4+Uv0=.ed25519'),
      isPublic()
    ),
    startFrom(0),
    paginate(25),
    descending(),
  ],
  'anothor author posts': [
    and(
      type('post'),
      author('@QlCTpvY7p9ty2yOFrv1WU1AE88aoQc4Y7wYal7PFc+w=.ed25519'),
      isPublic()
    ),
    startFrom(0),
    paginate(25),
    descending(),
  ],
}

const tests = Object.keys(queries).length

function runQuery() {
  const name = Object.keys(queries)[run]
  console.log('running:', name)
  console.time('query')

  sbot.db.query(
    ...queries[name],
    toCallback((err, msgs) => {
      //const results = msgs.results ? msgs.results : msgs
      console.timeEnd('query')
      if (++run >= tests) sbot.close()
      else runQuery()
    })
  )
}

runQuery()
