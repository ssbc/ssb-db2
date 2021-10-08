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
const fromEvent = require('pull-stream-util/from-event')
const { toCallback, and, where, isPrivate, type } = require('../operators')

const dir = '/tmp/ssb-db2-migrate'

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

test('migrate moves msgs from old log to new log', (t) => {
  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, { keys, path: dir })

  sbot.db2migrate.start()

  pull(
    fromEvent('ssb:db2:migrate:progress', sbot),
    pull.take(1),
    pull.collect((err, nums) => {
      t.error(err)
      t.equals(nums[nums.length - 1], 1, 'last progress event is one')

      // we need to make sure async-log has written the data
      sbot.db.getLog().onDrain(() => {
        t.true(
          fs.existsSync(path.join(dir, 'db2', 'log.bipf')),
          'migration done'
        )
        sbot.db.query(
          toCallback((err1, msgs) => {
            t.error(err1, 'no err')
            t.equal(msgs.length, TOTAL)
            sbot.close(t.end)
          })
        )
      })
    })
  )
})

test('migrate moves msgs from ssb-db to new log', (t) => {
  // Delete db2 folder to make sure we start migrating from zero
  rimraf.sync(path.join(dir, 'db2'))

  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('ssb-db'))
    .use(require('../'))
    .call(null, { keys, path: dir })

  sbot.db2migrate.start()

  pull(
    fromEvent('ssb:db2:migrate:progress', sbot),
    pull.take(1),
    pull.collect((err, nums) => {
      t.error(err)
      t.equals(nums[nums.length - 1], 1, 'last progress event is one')

      // we need to make sure async-log has written the data
      sbot.db.getLog().onDrain(() => {
        t.true(
          fs.existsSync(path.join(dir, 'db2', 'log.bipf')),
          'migration done'
        )
        sbot.db.query(
          toCallback((err1, msgs) => {
            t.error(err1, 'no err')
            t.equal(msgs.length, TOTAL)
            sbot.close(t.end)
          })
        )
      })
    })
  )
})

test('migrate keeps new log synced with ssb-db being updated', (t) => {
  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('ssb-db'))
    .use(require('../index'))
    .call(null, {
      keys,
      path: dir,
      db2: {
        automigrate: true,
      },
    })

  pull(
    fromEvent('ssb:db2:migrate:progress', sbot),
    pull.filter((x) => x === 1),
    pull.take(1),
    pull.drain(() => {
      t.true(fs.existsSync(path.join(dir, 'db2', 'log.bipf')), 'migration done')
      sbot.db.query(
        toCallback((err1, msgs) => {
          t.error(err1, '1st query suceeded')
          t.equal(msgs.length, TOTAL, `${TOTAL} msgs`)

          // This should run after the sbot.publish completes
          setTimeout(() => {
            sbot.db.query(
              toCallback((err3, msgs2) => {
                t.error(err3, '2nd query suceeded')
                t.equal(msgs2.length, TOTAL + 1, `${TOTAL + 1} msgs`)
                t.equal(msgs2[TOTAL].value.content.text, 'Extra post')
                sbot.close(t.end)
              })
            )
          }, 200)

          sbot.publish({ type: 'post', text: 'Extra post' }, (err2, posted) => {
            t.error(err2, 'publish suceeded')
            t.equals(posted.value.content.type, 'post', 'msg posted')
          })
        })
      )
    })
  )
})

test('test migrate with encrypted messages', (t) => {
  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('ssb-db'))
    .use(require('../index'))
    .call(null, {
      keys,
      path: dir,
      db2: {
        automigrate: true,
      },
    })

  let content = { type: 'post', text: 'super secret', recps: [keys.id] }
  content = ssbKeys.box(
    content,
    content.recps.map((x) => x.substr(1))
  )
  sbot.publish(content, (err, posted) => {
    t.error(err, 'publish suceeded')
    t.equals(typeof posted.value.content, 'string', 'private msg posted')
    sbot.db.query(
      where(and(type('post'), isPrivate())),
      toCallback((err, msgs) => {
        t.error(err, 'no err')
        t.equal(msgs.length, 2)
        t.equal(msgs[1].value.content.text, 'super secret')
        sbot.close(t.end)
      })
    )
  })
})

test('refuses to db2.add() while old log exists', (t) => {
  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('ssb-db'))
    .use(require('../index'))
    .call(null, { keys, path: dir, db2: { automigrate: true } })

  pull(
    fromEvent('ssb:db2:migrate:progress', sbot),
    pull.filter((x) => x === 1),
    pull.take(1),
    pull.drain(() => {
      t.pass('migration done')
      const post = { type: 'post', text: 'Testing!' }
      sbot.db.publish(post, (err, posted) => {
        t.ok(err)
        t.notOk(posted)
        t.true(
          err.message.includes('refusing to publish() because the old log'),
          'error message is about the old log'
        )
        sbot.close(t.end)
      })
    })
  )
})

test('regenerate fixture with flumelog-offset', (t) => {
  // delete previous
  rimraf.sync(dir)

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

test('dangerouslyKillFlumeWhenMigrated and refusing db2.publish()', (t) => {
  t.true(fs.existsSync(path.join(dir, 'flume')), 'flume folder exists')

  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../index'))
    .call(null, {
      keys,
      path: dir,
      db2: { automigrate: true, dangerouslyKillFlumeWhenMigrated: true },
    })

  sbot.db.publish({ type: 'post', text: 'queued' }, (err, posted) => {
    t.ok(err)
    t.notOk(posted)
    t.true(
      err.message.includes('refusing to publish()'),
      'error message is migration in progress'
    )
  })

  pull(
    fromEvent('ssb:db2:migrate:progress', sbot),
    pull.filter((x) => x === 1),
    pull.take(1),
    pull.drain(() => {
      t.pass('migration done')
      t.false(
        fs.existsSync(path.join(dir, 'flume')),
        'flume folder was deleted'
      )
      // Wait for queued publish calls to complete
      setTimeout(() => {
        sbot.db.query(
          toCallback((err1, msgs) => {
            t.error(err1, 'no err when querying')
            t.equal(msgs.length, TOTAL, `there are ${TOTAL} msgs`)
            sbot.close(t.end)
          })
        )
      }, 300)
    })
  )
})

test('migrate does nothing when there is no old log', (t) => {
  const emptyDir = '/tmp/ssb-db2-migrate-empty'
  rimraf.sync(emptyDir)
  mkdirp.sync(emptyDir)

  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../index'))
    .call(null, { keys: ssbKeys.generate(), path: emptyDir })

  sbot.db2migrate.start()

  setTimeout(() => {
    t.pass('did nothing')
    sbot.close(t.end)
  }, 1000)

  pull(
    fromEvent('ssb:db2:migrate:progress', sbot),
    pull.drain(() => {
      t.fail('we are not supposed to get any migrate progress events')
    })
  )
})
