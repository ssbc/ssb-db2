const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')

const dir = '/tmp/ssb-db2-ebt'

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
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.onDrain('ebt', () => {
      sbot.getAtSequence([keys.id, 1], (err, msg) => {
        t.equal(msg.value.content.text, postMsg.value.content.text)
        t.end()
      })
    })
  })
})

test('author sequence', (t) => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(post2, (err, postMsg2) => {
      t.error(err, 'no err')

      db.onDrain('ebt', () => {
        db.getIndex('ebt').getMessageFromAuthorSequence(
          [keys.id, postMsg2.value.sequence],
          (err, msg) => {
            t.error(err, 'no err')
            t.equal(msg.value.content.text, post2.text, 'correct msg')

            t.end()
          }
        )
      })
    })
  })
})

test('Encrypted', (t) => {
  let content = { type: 'post', text: 'super secret', recps: [keys.id] }
  content = ssbKeys.box(
    content,
    content.recps.map((x) => x.substr(1))
  )

  db.publish(content, (err) => {
    t.error(err, 'no err')

    db.onDrain('ebt', () => {
      sbot.getAtSequence([keys.id, 4], (err, msg) => {
        t.equal(msg.value.content, content)
        sbot.close(t.end)
      })
    })
  })
})
