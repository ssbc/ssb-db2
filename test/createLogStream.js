// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const ssbKeys = require('ssb-keys')
const path = require('path')
const test = require('tape')
const pull = require('pull-stream')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const SecretStack = require('secret-stack')
const timestamp = require('monotonic-timestamp')
const caps = require('ssb-caps')
const ref = require('ssb-ref')

const dir = '/tmp/ssb-db2-log-stream'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const sbot = SecretStack({ appKey: caps.shs })
  .use(require('../'))
  .use(require('../compat/log-stream'))
  .call(null, {
    keys,
    path: dir,
  })
const db = sbot.db

test('createLogStream (simple)', (t) => {
  db.publish({ type: 'post', text: 'First' }, (err) => {
    pull(
      sbot.createLogStream(),
      pull.collect((err2, ary) => {
        t.error(err2)
        t.equal(ary.length, 1)
        t.assert(!!ary[0].key)
        t.assert(!!ary[0].value)
        t.end()
      })
    )
  })
})

test('createLogStream (keys only)', function (t) {
  pull(
    sbot.createLogStream({ values: false }),
    pull.collect((err, ary) => {
      t.error(err)
      t.equal(ary.length, 1)
      t.equal(typeof ary[0], 'string')
      t.true(ref.isMsgId(ary[0]))
      t.end()
    })
  )
})

test('createLogStream (values only)', function (t) {
  pull(
    sbot.createLogStream({ keys: false }),
    pull.collect((err, ary) => {
      t.error(err)
      t.equal(ary.length, 1)
      t.equal(typeof ary[0].content.type, 'string')
      t.end()
    })
  )
})

test('createLogStream (live)', function (t) {
  t.plan(3)

  var ts = Date.now()

  pull(
    sbot.createLogStream({ live: true }),
    pull.drain(function (m) {
      if (m.sync) return t.pass('{sync: true}')
      t.true(m.timestamp > ts)
      t.equal(m.value.content.type, 'msg')
      t.end()
      return false // abort the pull.drain
    })
  )

  setTimeout(db.publish, 1000, { type: 'msg', text: 'Second' }, (err) => {
    if (err) t.fail(err)
  })
})

test('createLogStream (live, !sync)', function (t) {
  t.plan(2)

  var ts = Date.now()

  pull(
    sbot.createLogStream({ live: true, sync: false }),
    pull.drain(function (m) {
      if (m.sync) t.fail('there should be no {sync: true}')
      t.true(m.timestamp > ts)
      t.equal(m.value.content.type, 'food')
      t.end()
      return false // abort the pull.drain
    })
  )

  setTimeout(db.publish, 1000, { type: 'food', text: 'Third' }, (err) => {
    if (err) t.fail(err)
  })
})

test('createLogStream (reverse)', function (t) {
  pull(
    sbot.createLogStream({ reverse: false }),
    pull.collect((err, ary) => {
      t.error(err)
      t.equal(ary.length, 3)
      t.equal(ary[0].value.content.text, 'First')
      t.equal(ary[1].value.content.text, 'Second')
      t.equal(ary[2].value.content.text, 'Third')

      pull(
        sbot.createLogStream({ reverse: true }),
        pull.collect((err, ary) => {
          t.error(err)
          t.equal(ary.length, 3)
          t.equal(ary[0].value.content.text, 'Third')
          t.equal(ary[1].value.content.text, 'Second')
          t.equal(ary[2].value.content.text, 'First')
          t.end()
        })
      )
    })
  )
})

test('createLogStream (limit)', function (t) {
  pull(
    sbot.createLogStream({ limit: 1 }),
    pull.collect((err, ary) => {
      t.error(err)
      t.equal(ary.length, 1)
      t.end()
    })
  )
})

// TODO
test.skip('createLogStream (gt)', (t) => {
  const start = timestamp()
  db.publish({ type: 'post', text: 'Second' }, (err) => {
    pull(
      sbot.createLogStream({ gt: start }),
      pull.collect(function (err2, ary) {
        t.error(err2)
        t.equal(ary.length, 1)
        t.equal(ary[0].value.content.text, 'Second')
        t.end()
      })
    )
  })
})

test('teardown sbot', (t) => {
  sbot.close(true, () => t.end())
})
