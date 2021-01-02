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
