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
const generateFixture = require('ssb-fixtures')
const fs = require('fs')
const pull = require('pull-stream')

const dir = '/tmp/ssb-db2-migrate-alone'

rimraf.sync(dir)
mkdirp.sync(dir)

const TOTAL = 10

test('generate fixture with flumelog-offset', (t) => {
  generateFixture({
    outputDir: dir,
    seed: 'migrate',
    messages: TOTAL,
    authors: 5,
    slim: true,
  }).then(() => {
    t.true(
      fs.existsSync(path.join(dir, 'flume', 'log.offset')),
      'log.offset was created'
    )
    t.end()
  })
})

test('migrate (alone) moves msgs from old log to new log', (t) => {
  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../migrate'))
    .call(null, { keys, path: dir })

  sbot.db2migrate.start()

  pull(
    sbot.db2migrate.progress(),
    pull.filter((x) => x === 1),
    pull.take(1),
    pull.collect((err) => {
      t.error(err)
      setTimeout(() => {
        t.true(
          fs.existsSync(path.join(dir, 'db2', 'log.bipf')),
          'migration done'
        )
        sbot.close(() => {
          t.end()
        })
      }, 1000)
    })
  )
})
