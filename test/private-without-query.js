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

const dir = '/tmp/ssb-db2-private-without-query'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

let sbot = SecretStack({ appKey: caps.shs }).use(require('../')).call(null, {
  keys,
  path: dir,
})
const db = sbot.db

test('private index shows in status', (t) => {
  const content = { type: 'post', text: 'super secret', recps: [keys.id] }

  db.create({ content }, (err, privateMsg) => {
    t.error(err, 'no err')

    db.onDrain(() => {
      const stats = db.getStatus().value
      t.equal(stats.indexes.private, 0, 'status indexes.private exists')
      t.end()
    })
  })
})

test('teardown', (t) => {
  sbot.close(t.end)
})
