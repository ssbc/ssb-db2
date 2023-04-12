// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const bipf = require('bipf')
const bfe = require('ssb-bfe')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const classic = require('ssb-classic/format')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')

const dir = '/tmp/ssb-db2-ebt'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const sbot = SecretStack({ appKey: caps.shs })
  .use(require('../'))
  .use(require('../compat/ebt'))
  .use(require('ssb-buttwoo'))
  .call(null, {
    keys,
    path: dir,
  })
const db = sbot.db

test('Base', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    sbot.getAtSequence([keys.id, 1], (err, msg) => {
      t.error(err)
      t.equal(msg.value.content.text, postMsg.value.content.text)
      t.end()
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

      sbot.getAtSequence([keys.id, postMsg2.value.sequence], (err, msg) => {
        t.error(err, 'no err')
        t.equal(msg.value.content.text, post2.text, 'correct msg')

        t.end()
      })
    })
  })
})

test('vector clock', (t) => {
  sbot.getVectorClock((err, clock) => {
    t.error(err, 'no err')
    t.deepEquals(clock, { [keys.id]: 3 })

    t.end()
  })
})

test('Encrypted', (t) => {
  let content = { type: 'post', text: 'super secret', recps: [keys.id] }
  content = ssbKeys.box(
    content,
    content.recps.map((x) => x.substr(1))
  )

  let i = 0

  var remove = sbot.db.onMsgAdded((ev) => {
    const msg = ev.kvt
    if (i++ === 0) t.equal(msg.value.sequence, 3, 'we get existing')
    else {
      t.equal(msg.value.sequence, 4, 'post is called on publish')
      remove()
    }
  })

  db.publish(content, (err) => {
    t.error(err, 'no err')

    sbot.getAtSequence([keys.id, 4], (err, msg) => {
      t.error(err)
      t.equal(msg.value.content, content)
      t.end()
    })
  })
})

test('add', (t) => {
  const keys2 = ssbKeys.generate()

  const msgVal = classic.newNativeMsg({
    keys: keys2,
    content: { type: 'post', text: 'testing sbot.add' },
    previous: null,
    timestamp: Date.now(),
    tag: 0,
    hmacKey: null,
  })

  let i = 0

  var remove = sbot.db.onMsgAdded((ev) => {
    const msg = ev.kvt
    if (i++ === 0) t.equal(msg.value.author, keys.id, 'we get existing')
    else {
      t.equal(msg.value.author, keys2.id, 'post is called on add')
      remove()
    }
  })

  sbot.add(msgVal, (err, added) => {
    t.error(err)
    t.equal(added.value.content.text, 'testing sbot.add')
    t.end()
  })
})

test('buttwoo-v1 sequenceNativeMsg', (t) => {
  const buttwooKeys = ssbKeys.generate(null, null, 'buttwoo-v1')

  db.create(
    {
      feedFormat: 'buttwoo-v1',
      content: {
        type: 'post',
        text: 'I am buttwoo',
      },
      keys: buttwooKeys,
      parent: null,
      tag: 0,
    },
    (err, msg1) => {
      t.error(err, 'no err')

      sbot.getAtSequenceNativeMsg(
        [buttwooKeys.id, msg1.value.sequence],
        'buttwoo-v1',
        (err, nativeMsg) => {
          const layer1 = bipf.decode(nativeMsg)
          t.true(Array.isArray(layer1), 'layer1 is array')
          t.equal(layer1.length, 3, 'layer1 has 3 items')
          const [encodedValue, signature, contentBuf] = layer1
          const [authorBFE] = bipf.decode(encodedValue)
          t.true(bfe.isEncodedFeedButtwooV1(authorBFE), 'authorBFE is good')
          const content = bipf.decode(contentBuf)
          t.equal(content.text, 'I am buttwoo', 'correct msg')
          t.end()
        }
      )
    }
  )
})

test('teardown sbot', (t) => {
  sbot.close(true, () => t.end())
})
