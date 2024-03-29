// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

// add a bunch of message, add index see that it indexes correctly

// add messages after the index has been created, also test indexSync

const test = require('tape')
const ssbKeys = require('ssb-keys')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const pull = require('pull-stream')
const classic = require('ssb-classic/format')

const sbot = SecretStack({ appKey: caps.shs }).use(require('../')).call(null, {
  keys: ssbKeys.generate(),
  temp: 'ssb-db2-base-index',
})
const db = sbot.db

test('drain', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.onDrain('base', () => {
      const status = db.getStatus().value
      t.equal(status.log, 0, 'log in sync')
      t.equal(status.indexes['base'], 0, 'index in sync')
      t.end()
    })
  })
})

test('get', (t) => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(post2, (err, postMsg2) => {
      t.error(err, 'no err')

      db.get(postMsg.key, (err, msg) => {
        t.equal(msg.content.text, post.text, 'correct msg')

        t.end()
      })
    })
  })
})

test('get latest', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.onDrain('base', () => {
      db.getLatest(sbot.id, (err, status) => {
        t.error(err, 'no err')
        t.equal(status.sequence, postMsg.value.sequence)
        t.true(status.offset > 100)

        t.end()
      })
    })
  })
})

test('get all latest', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.onDrain('base', () => {
      pull(
        db.getAllLatest(),
        pull.collect((err, all) => {
          t.error(err, 'no err')
          t.equals(all.length, 1)
          const { key, value } = all[0]
          t.equal(key, sbot.id)
          t.equal(value.sequence, postMsg.value.sequence)
          t.true(value.offset > 100)

          t.end()
        })
      )
    })
  })
})

test('get latest ooo', (t) => {
  const keys2 = ssbKeys.generate()

  const msgVal = classic.newNativeMsg({
    keys: keys2,
    content: { type: 'post', text: 'testing ooo' },
    previous: null,
    timestamp: Date.now(),
    tag: 0,
    hmacKey: null,
  })

  db.addOOO(msgVal, (err) => {
    t.error(err, 'no err')

    db.onDrain('base', () => {
      db.getLatest(keys2.id, (err, status) => {
        t.ok(err, 'should err on unknown feed')

        t.end()
      })
    })
  })
})

test('get latest ooo transaction', (t) => {
  const keys1 = ssbKeys.generate()
  const keys2 = ssbKeys.generate()

  const msgVal = classic.newNativeMsg({
    keys: keys1,
    content: { type: 'post', text: 'normal msg' },
    previous: null,
    timestamp: Date.now(),
    tag: 0,
    hmacKey: null,
  })

  const oooMsgVal = classic.newNativeMsg({
    keys: keys2,
    content: { type: 'post', text: 'ooo in another feed' },
    previous: null,
    timestamp: Date.now(),
    tag: 0,
    hmacKey: null,
  })

  db.addTransaction([msgVal], [oooMsgVal], (err) => {
    t.error(err, 'no err')

    db.onDrain('base', () => {
      pull(
        db.getAllLatest(),
        pull.collect((err, all) => {
          t.error(err, 'no err')

          // must include sbot.id + keys1.id
          t.equals(all.length, 2)

          const sbotLatest = all.find(x => x.key === sbot.id)
          t.equal(sbotLatest.value.sequence, 5)

          const keys1Latest = all.find(x => x.key === keys1.id)
          t.equal(keys1Latest.value.sequence, 1)
          t.true(keys1Latest.value.offset > 100)

          sbot.close(t.end)
        })
      )
    })
  })
})
