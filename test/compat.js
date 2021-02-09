const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const { toCallback } = require('../operators')

const dir = '/tmp/ssb-db2-compat-basic'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

let sbot = SecretStack({ appKey: caps.shs })
  .use(require('../'))
  .use(require('../compat/db'))
  .call(null, {
    keys,
    path: dir,
  })

test('publish', (t) => {
  sbot.publish({ type: 'post', text: 'sbot.publish test' }, (err) => {
    t.error(err, 'no err')
    sbot.db.query(
      toCallback((err2, msgs) => {
        t.equal(msgs.length, 1)
        t.equal(msgs[0].value.content.text, 'sbot.publish test')
        t.end()
      })
    )
  })
})

test('whoami', (t) => {
  const resp = sbot.whoami()
  t.equal(typeof resp, 'object')
  t.equal(typeof resp.id, 'string')
  t.equal(resp.id, sbot.id)
  t.end()
})

test('ready', (t) => {
  t.equal(sbot.ready(), true)
  t.end()
})

test('teardown sbot', (t) => {
  sbot.close(t.end)
})
