// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const ssbKeys = require('ssb-keys')
const classic = require('ssb-classic/format')
const path = require('path')
const test = require('tape')
const pull = require('pull-stream')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')

const dir = '/tmp/ssb-db2-history-stream'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const sbot = SecretStack({ appKey: caps.shs })
  .use(require('../'))
  .use(require('../compat/db'))
  .use(require('../compat/history-stream'))
  .call(null, {
    keys,
    path: dir,
  })
const db = sbot.db

test('Base', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  const otherKeys = ssbKeys.generate()

  const msgVal = classic.newNativeMsg({
    keys: otherKeys,
    content: { type: 'post', text: 'test1' },
    previous: null,
    timestamp: Date.now(),
    hmacKey: null,
  })

  db.add(msgVal, (err) => {
    db.publish(post, (err, postMsg) => {
      pull(
        sbot.createHistoryStream({ id: keys.id, keys: false }),
        pull.collect((err, results) => {
          t.equal(results.length, 1)
          // values directly
          t.equal(results[0].content.text, post.text)
          t.end()
        })
      )
    })
  })
})

test('Keys', (t) => {
  pull(
    sbot.createHistoryStream({ id: keys.id }),
    pull.collect((err, results) => {
      t.equal(results.length, 1)
      t.equal(typeof results[0].key, 'string')
      t.end()
    })
  )
})

test('No values', (t) => {
  pull(
    sbot.createHistoryStream({ id: keys.id, values: false }),
    pull.collect((err, results) => {
      t.equal(results.length, 1)
      t.equal(typeof results[0], 'string')
      t.end()
    })
  )
})

test('createWriteStream', (t) => {
  const rando = ssbKeys.generate()

  const msg1 = classic.newNativeMsg({
    keys: rando,
    content: { type: 'post', text: 'a' },
    previous: null,
    timestamp: Date.now() - 3,
    hmacKey: null,
  })
  const msg2 = classic.newNativeMsg({
    keys: rando,
    content: { type: 'post', text: 'b' },
    previous: { key: classic.getMsgId(msg1), value: msg1 },
    timestamp: Date.now() - 2,
    hmacKey: null,
  })
  const msg3 = classic.newNativeMsg({
    keys: rando,
    content: { type: 'post', text: 'c' },
    previous: { key: classic.getMsgId(msg2), value: msg2 },
    timestamp: Date.now() - 1,
    hmacKey: null,
  })

  let wrote = 0
  pull(
    pull.values([msg1, msg2, msg3]),
    pull.through(() => {
      wrote++
    }),
    sbot.createWriteStream((err) => {
      t.error(err)
      t.equals(wrote, 3)
      pull(
        sbot.createHistoryStream({ id: rando.id, values: true }),
        pull.collect((err2, results) => {
          t.equals(results.length, 3)
          t.equal(results[0].value.content.text, 'a')
          t.equal(results[1].value.content.text, 'b')
          t.equal(results[2].value.content.text, 'c')
          t.end()
        })
      )
    })
  )
})

test('Seq', (t) => {
  pull(
    sbot.createHistoryStream({ id: keys.id, keys: false, seq: 1 }),
    pull.collect((err, results) => {
      t.equal(results.length, 1)

      pull(
        sbot.createHistoryStream({ id: keys.id, keys: false, seq: 0 }),
        pull.collect((err, results) => {
          t.equal(results.length, 1)

          const post = { type: 'post', text: 'Testing 2' }
          db.publish(post, (err, postMsg) => {
            pull(
              sbot.createHistoryStream({ id: keys.id, keys: false, seq: 2 }),
              pull.collect((err, results) => {
                t.equal(results.length, 1)
                t.equal(results[0].content.text, post.text)

                pull(
                  sbot.createHistoryStream({
                    id: keys.id,
                    keys: false,
                    seq: 1,
                    limit: 1,
                  }),
                  pull.collect((err, results) => {
                    t.equal(results.length, 1)
                    t.equal(results[0].content.text, 'Testing!')

                    t.end()
                  })
                )
              })
            )
          })
        })
      )
    })
  )
})

test('limit', (t) => {
  db.publish({ type: 'post', text: 'Testing 3' }, (err, postMsg) => {
    pull(
      sbot.createHistoryStream({ id: keys.id, limit: 1 }),
      pull.collect((err, results) => {
        t.equal(results.length, 1)
        t.equal(results[0].value.content.text, 'Testing!')

        pull(
          sbot.createHistoryStream({ id: keys.id }),
          pull.collect((err, results) => {
            t.equal(results.length, 3)
            t.end()
          })
        )
      })
    )
  })
})

test('non feed should err', (t) => {
  pull(
    sbot.createHistoryStream({ id: 'wat', limit: 1 }),
    pull.collect((err, results) => {
      t.equal(err.message, 'wat is not a feed')
      t.end()
    })
  )
})

test('Encrypted', (t) => {
  let content = { type: 'post', text: 'super secret', recps: [keys.id] }
  content = ssbKeys.box(
    content,
    content.recps.map((x) => x.substr(1))
  )

  db.publish(content, (err, privateMsg) => {
    pull(
      sbot.createHistoryStream({ id: keys.id, keys: false }),
      pull.collect((err, results) => {
        t.equal(results.length, 4)
        t.equal(typeof results[3].content, 'string')
        sbot.close(t.end)
      })
    )
  })
})

test('should be hookable', (t) => {
  let hookCalled = false
  sbot.createHistoryStream.hook(function (fn, args) {
    hookCalled = true
    return fn.call(null, args[0])
  })

  pull(
    sbot.createHistoryStream({ id: 'wat', limit: 1 }),
    pull.collect(() => {
      t.true(hookCalled)
      t.end()
    })
  )
})
