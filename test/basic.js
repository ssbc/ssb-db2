const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const validate = require('ssb-validate')
const pull = require('pull-stream')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')

const dir = '/tmp/ssb-db2-basic'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const sbot = SecretStack({ appKey: caps.shs })
      .use(require('../'))
      .use(require('../compat/ebt'))
      .call(null, {
  keys,
  path: dir,
})
const db = sbot.db

test('Base', (t) => {
  const posts = []
  for (var i = 0; i < 1000; ++i)
    posts.push({ type: 'post', text: 'Testing!' })

  let j = 0
  
  pull(
    pull.values(posts),
    pull.asyncMap(db.publish),
    pull.asyncMap((postMsg, cb) => {
      if (j++ % 3 === 0) {
        db.onDrain('base', () => {
          sbot.getAtSequence([keys.id, j], (err, msg) => {
            t.equal(msg.key, postMsg.key)
            cb(err)
          })
        })
      } else
        cb()
    }),
    pull.collect((err) => {
      sbot.close(t.end)
    })
  )
})
