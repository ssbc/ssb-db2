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
const fromEvent = require('pull-stream-util/from-event')

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
    fromEvent('ssb:db2:migrate:progress', sbot),
    pull.take(TOTAL),
    pull.collect((err, nums) => {
      t.error(err)
      t.equals(nums.length, TOTAL, `${TOTAL} progress events emitted`)
      t.equals(nums[0], 0, 'first progress event is zero')
      t.true(nums[0] < nums[1], 'monotonically increasing')
      t.true(nums[1] < nums[2], 'monotonically increasing')
      t.equals(nums[TOTAL - 1], 1, 'last progress event is one')
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
