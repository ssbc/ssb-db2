// SPDX-FileCopyrightText: 2023 Andre 'Staltz' Medeiros <contact@staltz.com>
//
// SPDX-License-Identifier: CC0-1.0

const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const pify = require('util').promisify
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const classic = require('ssb-classic/format')

const dir = '/tmp/ssb-db2-ooo'

rimraf.sync(dir)
mkdirp.sync(dir)

test('addOOO, deleteFeed, add, deleteFeed', async (t) => {
  const bobKeys = ssbKeys.generate('ed25519', 'bob')
  const bobMsgs = []

  const now = Date.now()
  bobMsgs.push(
    classic.newNativeMsg({
      keys: bobKeys,
      timestamp: now,
      content: { type: 'post', text: 'one' },
    })
  )
  bobMsgs.push(
    classic.newNativeMsg({
      keys: bobKeys,
      timestamp: now + 1,
      content: { type: 'post', text: 'two' },
      previous: { key: classic.getMsgId(bobMsgs[0]), value: bobMsgs[0] },
    })
  )
  bobMsgs.push(
    classic.newNativeMsg({
      keys: bobKeys,
      timestamp: now + 2,
      content: { type: 'post', text: 'three' },
      previous: { key: classic.getMsgId(bobMsgs[1]), value: bobMsgs[1] },
    })
  )

  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, {
      keys,
      path: dir,
    })

  await pify(sbot.db.addOOO)(bobMsgs[0])
  await pify(sbot.db.addOOO)(bobMsgs[2])
  t.pass('added two out-of-order messages from bob')

  const bobMsgKey0 = classic.getMsgId(bobMsgs[0])
  const bobMsgKey1 = classic.getMsgId(bobMsgs[1])
  const bobMsgKey2 = classic.getMsgId(bobMsgs[2])

  const bobKVT0 = await pify(sbot.db.getMsg)(bobMsgKey0)
  t.equals(bobKVT0.value.content.text, 'one', 'first msg is correct')
  try {
    await pify(sbot.db.getMsg)(bobMsgKey1)
    t.fail('getMsg should have failed')
  } catch (err) {
    t.match(err.message, /not found/, 'second msg is missing')
  }
  const bobKVT2 = await pify(sbot.db.getMsg)(bobMsgKey2)
  t.equals(bobKVT2.value.content.text, 'three', 'third msg is correct')

  await pify(sbot.db.deleteFeed)(bobKeys.id)
  t.pass("deleted all of bob's messages")

  try {
    await pify(sbot.db.getMsg)(bobMsgKey2)
    t.fail('getMsg should have failed')
  } catch (err) {
    t.match(err.message, /not found/, 'bob messages are gone')
  }

  await pify(sbot.db.add)(bobMsgs[0])
  await pify(sbot.db.add)(bobMsgs[1])
  await pify(sbot.db.add)(bobMsgs[2])
  t.pass('added three messages from bob')

  const bobKVT0Again = await pify(sbot.db.getMsg)(bobMsgKey0)
  t.equals(bobKVT0Again.value.content.text, 'one', 'first msg is correct')
  const bobKVT1Again = await pify(sbot.db.getMsg)(bobMsgKey1)
  t.equals(bobKVT1Again.value.content.text, 'two', 'second msg is correct')
  const bobKVT2Again = await pify(sbot.db.getMsg)(bobMsgKey2)
  t.equals(bobKVT2Again.value.content.text, 'three', 'third msg is correct')

  await pify(sbot.db.deleteFeed)(bobKeys.id)
  t.pass("deleted all of bob's messages")

  try {
    await pify(sbot.db.getMsg)(bobMsgKey2)
    t.fail('getMsg should have failed')
  } catch (err) {
    t.match(err.message, /not found/, 'bob messages are gone')
  }

  await pify(sbot.close)(true)
  t.end()
})
