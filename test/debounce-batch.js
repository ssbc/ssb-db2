// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const pify = require('util').promisify
const classic = require('ssb-classic/format')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')

const dir = '/tmp/ssb-db2-debounce-batch'

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

test('add many times', async (t) => {
  const keys1 = ssbKeys.generate()
  const keys2 = ssbKeys.generate()

  let prev1 = null
  let prev2 = null
  const queue = []
  // 1..99, inclusive
  for (let i = 1; i <= 99; ++i) {
    if (i % 2 === 0) {
      const msgVal = classic.newNativeMsg({
        keys: keys1,
        content: { type: 'post', text: 'a' + i },
        previous: prev1,
        timestamp: Date.now(),
        hmacKey: null,
      })
      queue.push(msgVal)
      prev1 = { key: classic.getMsgId(msgVal), value: msgVal }
    } else {
      const msgVal = classic.newNativeMsg({
        keys: keys2,
        content: { type: 'post', text: 'b' + i },
        previous: prev2,
        timestamp: Date.now(),
        hmacKey: null,
      })
      queue.push(msgVal)
      prev2 = { key: classic.getMsgId(msgVal), value: msgVal }
    }
  }

  await Promise.all(queue.map((msgVal) => pify(sbot.add)(msgVal)))
  t.pass('added messages by two authors')

  await pify(setTimeout)(1000)

  const msgs = await new Promise((resolve, reject) => {
    sbot.db.query(
      sbot.db.operators.toCallback((err, msgs) => {
        if (err) reject(err)
        else resolve(msgs)
      })
    )
  })

  t.equals(msgs.length, 99, 'there are 99 messages')
  const msgs1 = msgs.filter((msg) => msg.value.author === keys1.id)
  const msgs2 = msgs.filter((msg) => msg.value.author === keys2.id)
  t.equals(msgs1.length, 49, 'there are 49 messages by author1')
  t.equals(msgs2.length, 50, 'there are 50 messages by author2')

  const finalMsgVal = classic.newNativeMsg({
    keys: keys1,
    content: { type: 'post', text: 'a' + 100 },
    previous: prev1,
    timestamp: Date.now(),
    hmacKey: null,
  })
  const added = await pify(sbot.add)(finalMsgVal)
  t.deepEquals(added.value, finalMsgVal)

  await pify(sbot.close)(true)
  t.end()
})
