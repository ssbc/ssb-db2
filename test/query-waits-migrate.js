const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const generateFixture = require('ssb-fixtures')
const fs = require('fs')
const { and, type, descending, paginate, toCallback } = require('../operators')

const dir = '/tmp/ssb-db2-query-waits-migrate'

rimraf.sync(dir)
mkdirp.sync(dir)

const TOTAL = 1000

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

test('query() waits for migrate to sync old and new logs', (t) => {
  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, { keys, path: dir })

  sbot.db2migrate.start()

  sbot.db.query(
    and(type('post')),
    descending(),
    paginate(1),
    toCallback((err, response) => {
      t.error(err)
      t.true(response.total > 0, 'total > 0')
      t.ok(response.results[0])
      t.true(response.results[0].value.content.text.includes('LATESTMSG'))
      sbot.close(() => {
        t.end()
      })
    })
  )
})

test.skip('config.maxCpu makes indexing last longer', (t) => {
  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot1 = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, { keys, path: dir, db2: { maxCpu: Infinity } })

  sbot1.db2migrate.start()

  const start1 = Date.now()
  sbot1.db.query(
    and(type('post')),
    descending(),
    paginate(1),
    toCallback(() => {
      const duration1 = Date.now() - start1

      sbot1.close(() => {
        rimraf.sync(path.join(dir, 'db2'))

        const sbot2 = SecretStack({ appKey: caps.shs })
          .use(require('../'))
          .call(null, { keys, path: dir, db2: { maxCpu: 10 } })

        sbot2.db2migrate.start()

        const start2 = Date.now()
        sbot2.db.query(
          and(type('post')),
          descending(),
          paginate(1),
          toCallback(() => {
            const duration2 = Date.now() - start2

            t.pass('duration2 = ' + duration2 + ', duration1 = ' + duration1)
            t.true(duration2 > duration1, 'duration2 > duration1')
            t.true(duration2 > 2 * duration1, 'duration2 > 2 * duration1')

            sbot2.close(() => {
              t.end()
            })
          })
        )
      })
    })
  )
})
