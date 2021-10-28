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

const dir = '/tmp/ssb-db2-private'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const sbot = SecretStack({ appKey: caps.shs }).use(require('../')).call(null, {
  keys,
  path: dir,
})
const db = sbot.db

test('publish encrypted message', (t) => {
  let content = { type: 'post', text: 'super secret', recps: [keys.id] }
  content = ssbKeys.box(
    content,
    content.recps.map((x) => x.substr(1))
  )

  db.publish(content, (err, privateMsg) => {
    t.error(err, 'no err')

    t.equal(typeof privateMsg.value.content, 'string')
    db.get(privateMsg.key, (err, msg) => {
      t.equal(msg.content.text, 'super secret')
      t.end()
    })
  })
})

test('publish: auto encrypt message with recps', (t) => {
  let content = { type: 'post', text: 'super secret', recps: [keys.id] }

  db.publish(content, (err, privateMsg) => {
    t.error(err, 'no err')

    t.equal(typeof privateMsg.value.content, 'string')
    db.get(privateMsg.key, (err, msg) => {
      t.equal(msg.content.text, 'super secret')
      t.end()
    })
  })
})

test('publishAs classic', (t) => {
  let content = { type: 'post', text: 'super secret', recps: [keys.id] }

  db.publishAs(keys, content, (err, privateMsg) => {
    t.error(err, 'no err')

    t.equal(typeof privateMsg.value.content, 'string')
    db.get(privateMsg.key, (err, msg) => {
      t.equal(msg.content.text, 'super secret')
      sbot.close(t.end)
    })
  })
})
