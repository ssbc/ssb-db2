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
const { toCallback } = require('../operators')

const dir = '/tmp/ssb-db2-migration'

rimraf.sync(dir)
mkdirp.sync(dir)

const TOTAL = 10

test('generate fixture with flumelog-offset', (t) => {
  generateFixture({
    outputDir: dir,
    seed: 'migration',
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

test('migration moves msgs from old log to new log', (t) => {
  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../index'))
    .use(require('../migration'))
    .call(null, { keys, path: dir })

  sbot.dbMigration.start()

  let progressEventsReceived = false
  pull(
    fromEvent('ssb:db2:migration:progress', sbot),
    pull.take(TOTAL),
    pull.collect((err, nums) => {
      t.error(err)
      t.equals(nums.length, TOTAL, `${TOTAL} progress events emitted`)
      t.equals(nums[0], 0, 'first progress event is zero')
      t.true(nums[0] < nums[1], 'monotonically increasing')
      t.true(nums[1] < nums[2], 'monotonically increasing')
      t.equals(nums[TOTAL - 1], 1, 'last progress event is one')
      progressEventsReceived = true
    })
  )

  pull(
    fromEvent('ssb:db2:migration:progress', sbot),
    pull.filter((x) => x === 1),
    pull.take(1),
    // FIXME: why do we still need a setTimeout?
    pull.asyncMap((x, cb) => setTimeout(cb, 500)),
    pull.drain(() => {
      t.true(fs.existsSync(path.join(dir, 'db2', 'log.bipf')), 'migration done')
      sbot.db.onDrain(() => {
        sbot.db.query(
          toCallback((err1, msgs) => {
            t.error(err1, 'no err')
            t.equal(msgs.length, TOTAL)
            t.true(progressEventsReceived, 'progress events received')
            sbot.close(() => {
              t.end()
            })
          })
        )
      })
    })
  )
})

test('migration keeps new log synced with old log being updated', (t) => {
  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('ssb-db'))
    .use(require('../index'))
    .use(require('../migration'))
    .call(null, { keys, path: dir, db2: { automigrate: true } })

  pull(
    fromEvent('ssb:db2:migration:progress', sbot),
    pull.filter((x) => x === 1),
    pull.take(1),
    // FIXME: why do we still need a setTimeout?
    pull.asyncMap((x, cb) => setTimeout(cb, 500)),
    pull.drain(() => {
      t.true(fs.existsSync(path.join(dir, 'db2', 'log.bipf')), 'migration done')
      sbot.db.onDrain(() => {
        sbot.db.query(
          toCallback((err1, msgs) => {
            t.error(err1, '1st query suceeded')
            t.equal(msgs.length, TOTAL, `${TOTAL} msgs`)

            // This should run after the sbot.publish completes
            pull(
              fromEvent('ssb:db2:migration:progress', sbot),
              pull.filter((x) => x === 1),
              pull.take(1),
              // FIXME: why do we still need a setTimeout?
              pull.asyncMap((x, cb) => setTimeout(cb, 500)),
              pull.drain(() => {
                sbot.db.query(
                  toCallback((err3, msgs2) => {
                    t.error(err3, '2nd query suceeded')
                    t.equal(msgs2.length, TOTAL + 1, `${TOTAL + 1} msgs`)
                    sbot.close(() => {
                      t.end()
                    })
                  })
                )
              })
            )

            sbot.publish(
              { type: 'post', text: 'Extra post' },
              (err2, posted) => {
                t.error(err2, 'publish suceeded')
                t.equals(posted.value.content.type, 'post', 'msg posted')
              }
            )
          })
        )
      })
    })
  )
})

test('migration does nothing when there is no old log', (t) => {
  const emptyDir = '/tmp/ssb-db2-migration-empty'
  rimraf.sync(emptyDir)
  mkdirp.sync(emptyDir)

  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../index'))
    .use(require('../migration'))
    .call(null, { keys: ssbKeys.generate(), path: emptyDir })

  sbot.dbMigration.start()

  setTimeout(() => {
    sbot.close(() => {
      t.end()
    })
  }, 1000)

  pull(
    fromEvent('ssb:db2:migration:progress', sbot),
    pull.drain(() => {
      t.fail('we are not supposed to get any migration progress events')
    })
  )
})
