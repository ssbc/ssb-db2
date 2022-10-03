// SPDX-FileCopyrightText: 2022 Mix Irving
//
// SPDX-License-Identifier: Unlicense

const test = require('tape')
const ssbKeys = require('ssb-keys')
const rimraf = require('rimraf')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')

test('minimal db2 (no encryption)', t => {
  const dir = '/tmp/ssb-db2-minimal-create'
  rimraf.sync(dir)

  const keys = ssbKeys.generate()

  const stack = SecretStack({ appKey: caps.shs })
    .use(require('../core'))
    .use(require('ssb-classic'))

  const ssb = stack({
    path: dir,
    keys
  })

  ssb.db.create({
    content: { type: 'boop' },
    keys
  }, (err, msg) => {
    t.error(err, 'published message')

    ssb.db.create({
      content: { type: 'boop', recps: [ssb.id] },
      keys
    }, (err) => {
      t.match(err.message, /does not support encryption format undefined/, 'still errors on unencrypted messages')

      ssb.close(true, t.end)
    })
  })
})
