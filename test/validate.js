// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const classic = require('ssb-classic/format')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')

const dir = '/tmp/ssb-db2-validate'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const sbot = SecretStack({ appKey: caps.shs }).use(require('../')).call(null, {
  keys,
  path: dir,
})
const db = sbot.db

test('Base', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')
    t.equal(postMsg.value.content.text, post.text, 'text correct')
    t.end()
  })
})

test('Multiple', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    const post2 = { type: 'post', text: 'Testing 2!' }

    db.publish(post2, (err, postMsg2) => {
      t.error(err, 'no err')
      t.equal(postMsg2.value.content.text, post2.text, 'text correct')
      t.end()
    })
  })
})

test('Raw feed with unused type + ooo in batch', (t) => {
  const keys = ssbKeys.generate()

  const msg1 = classic.newNativeMsg({
    keys,
    content: { type: 'post', text: 'test1' },
    previous: null,
    timestamp: Date.now(),
    hmacKey: null,
  })
  const msg2 = classic.newNativeMsg({
    keys,
    content: { type: 'post', text: 'test2' },
    previous: { key: classic.getMsgId(msg1), value: msg1 },
    timestamp: Date.now() + 1,
    hmacKey: null,
  })
  const msg3 = classic.newNativeMsg({
    keys,
    content: { type: 'post', text: 'test3' },
    previous: { key: classic.getMsgId(msg2), value: msg2 },
    timestamp: Date.now() + 2,
    hmacKey: null,
  })
  const msg4 = classic.newNativeMsg({
    keys,
    content: { type: 'vote', vote: { link: '%something.sha256', value: 1 } },
    previous: { key: classic.getMsgId(msg3), value: msg3 },
    timestamp: Date.now() + 3,
    hmacKey: null,
  })
  const msg5 = classic.newNativeMsg({
    keys,
    content: { type: 'post', text: 'test5' },
    previous: { key: classic.getMsgId(msg4), value: msg4 },
    timestamp: Date.now() + 4,
    hmacKey: null,
  })
  const msg6 = classic.newNativeMsg({
    keys,
    content: { type: 'post', text: 'test6' },
    previous: { key: classic.getMsgId(msg5), value: msg5 },
    timestamp: Date.now() + 5,
    hmacKey: null,
  })

  const latestPost = db.post.value

  const msgVals = [msg3, msg4, msg5, msg6]
  db.addOOOBatch(msgVals, (err) => {
    t.error(err, 'no err')

    db.addOOO(msg1, (err, oooMsg) => {
      t.error(err, 'no err')
      t.equal(oooMsg.value.content.text, 'test1', 'text correct')
      t.equal(db.post.value, latestPost, 'ooo methods does not update post') // as that would break EBT

      t.end()
    })
  })
})

// we might get some messages from an earlier thread, and then get the
// latest 25 messages from the user
test('Add OOO with holes', (t) => {
  const keys = ssbKeys.generate()

  const msg1 = classic.newNativeMsg({
    keys,
    content: { type: 'post', text: 'test1' },
    previous: null,
    timestamp: Date.now(),
    hmacKey: null,
  })
  const msg2 = classic.newNativeMsg({
    keys,
    content: { type: 'post', text: 'test2' },
    previous: { key: classic.getMsgId(msg1), value: msg1 },
    timestamp: Date.now() + 1,
    hmacKey: null,
  })
  const msg3 = classic.newNativeMsg({
    keys,
    content: { type: 'post', text: 'test3' },
    previous: { key: classic.getMsgId(msg2), value: msg2 },
    timestamp: Date.now() + 2,
    hmacKey: null,
  })

  db.addOOO(msg1, (err) => {
    t.error(err, 'no err')

    db.addOOO(msg3, (err, msg) => {
      t.error(err, 'no err')
      t.equal(msg.value.content.text, 'test3', 'text correct')
      t.end()
    })
  })
})

test('Add same message twice', (t) => {
  const keys = ssbKeys.generate()

  const msg1 = classic.newNativeMsg({
    keys,
    content: { type: 'post', text: 'test1' },
    previous: null,
    timestamp: Date.now(),
    hmacKey: null,
  })
  const msg2 = classic.newNativeMsg({
    keys,
    content: { type: 'post', text: 'test2' },
    previous: { key: classic.getMsgId(msg1), value: msg1 },
    timestamp: Date.now() + 1,
    hmacKey: null,
  })

  db.add(msg1, (err) => {
    t.error(err, 'no err')

    db.add(msg2, (err) => {
      t.error(err, 'no err')

      // validate makes sure we can't add the same message twice
      db.add(msg2, (err) => {
        t.ok(err, 'Should fail to add')
        t.end()
      })
    })
  })
})

test('add fail case', (t) => {
  const keys = ssbKeys.generate()

  const msg1 = classic.newNativeMsg({
    keys,
    content: { type: 'post', text: 'test1' },
    previous: null,
    timestamp: Date.now(),
    hmacKey: null,
  })
  const msg2 = classic.newNativeMsg({
    keys,
    content: { type: 'post', text: 'test2' },
    previous: { key: classic.getMsgId(msg1), value: msg1 },
    timestamp: Date.now() + 1,
    hmacKey: null,
  })
  const msg3 = classic.newNativeMsg({
    keys,
    content: { type: 'post', text: 'test3' },
    previous: { key: classic.getMsgId(msg2), value: msg2 },
    timestamp: Date.now() + 2,
    hmacKey: null,
  })

  db.add(msg1, (err) => {
    t.error(err, 'no err')

    db.add(msg3, (err) => {
      t.ok(err, 'Should fail to add')

      sbot.close(t.end)
    })
  })
})
