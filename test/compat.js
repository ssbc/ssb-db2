// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const pull = require('pull-stream')
const { toCallback } = require('../operators')

const dir = '/tmp/ssb-db2-compat-basic'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const sbot = SecretStack({ appKey: caps.shs })
  .use(require('../'))
  .use(require('../compat/db'))
  .use(require('../compat/history-stream'))
  .use(require('../compat/post'))
  .call(null, {
    keys,
    path: dir,
  })

const post1 = { type: 'post', text: 'sbot.publish test' }
const post2 = { type: 'post', text: 'sbot.createHistoryStream live test' }

test('publish', (t) => {
  sbot.publish(post1, (err) => {
    t.error(err, 'no err')
    sbot.db.query(
      toCallback((err2, msgs) => {
        t.equal(msgs.length, 1)
        t.deepEqual(msgs[0].value.content, post1)
        t.end()
      })
    )
  })
})

test('createHistoryStream', t => {
  t.plan(6)

  pull(
    sbot.createHistoryStream({ id: sbot.id }),
    pull.map(m => m.value.content),
    pull.collect((err, res) => {
      if (err) t.error(err)
      t.deepEqual(res, [post1], 'createHistoryStream')

      /* start listening */
      let count = 0
      pull(
        sbot.createHistoryStream({ id: sbot.id, live: true }),
        pull.map(m => m.value.content),
        pull.drain(content => {
          count++
          if (count === 1) t.deepEqual(content, post1, 'createHistoryStream (live)')
          if (count === 2) t.deepEqual(content, post2, 'createHistoryStream (live)')
        })
      )

      sbot.publish(post2, (err) => {
        t.error(err, 'publish')

        pull(
          sbot.createHistoryStream({ id: sbot.id, seq: 2 }),
          pull.map(m => m.value.content),
          pull.collect((err, res) => {
            if (err) t.error(err)
            t.deepEqual(res, [post2], 'createHistoryStream (seq)')

            pull(
              sbot.createHistoryStream({ id: sbot.id, limit: 1 }),
              pull.map(m => m.value.content),
              pull.collect((err, res) => {
                if (err) t.error(err)
                t.deepEqual(res, [post1], 'createHistoryStream (limit)')
              })
            )
          })
        )
      })
    })
  )
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

test('keys', (t) => {
  t.deepEqual(sbot.keys, keys)
  t.end()
})

test('post', t=> {
  sbot.post((msg)=> {
    if (msg.value.content.text === 'post test') {
      t.end()
    }
  })

  sbot.publish({ type: 'test', text: 'post test'}, (err) => {
    if (err) t.fail(err, 'failed publish for post')
  })
})

test('teardown sbot', (t) => {
  sbot.close(true, () => t.end())
})
