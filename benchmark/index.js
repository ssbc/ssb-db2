const test = require('tape')
const fs = require('fs')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const generateFixture = require('ssb-fixtures')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const ssbKeys = require('ssb-keys')
const { and, type, descending, paginate, toCallback } = require('../operators')

const dir = '/tmp/ssb-db2-benchmark'
const oldLogPath = path.join(dir, 'flume', 'log.offset')
const db2Path = path.join(dir, 'db2')
const reportPath = path.join(dir, 'benchmark.md')

const skipCreate = process.argv[2] === 'noCreate'

if (!skipCreate) {
  rimraf.sync(dir)
  mkdirp.sync(dir)
  fs.appendFileSync(reportPath, '## Benchmark results\n\n')

  const SEED = 'sloop'
  const MESSAGES = 100000
  const AUTHORS = 2000

  test('generate fixture with flumelog-offset', (t) => {
    generateFixture({
      outputDir: dir,
      seed: SEED,
      messages: MESSAGES,
      authors: AUTHORS,
      slim: true,
    }).then(() => {
      t.pass(`seed = ${SEED}`)
      t.pass(`messages = ${MESSAGES}`)
      t.pass(`authors = ${AUTHORS}`)
      t.true(fs.existsSync(oldLogPath), 'log.offset was created')
      t.end()
    })
  })
}

test('initial indexing', (t) => {
  rimraf.sync(db2Path)

  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, {
      keys,
      path: dir,
    })

  sbot.db2migrate.start()

  const start = Date.now()
  sbot.db.query(
    and(type('post')),
    descending(),
    paginate(1),
    toCallback((err, { results, total }) => {
      const duration = Date.now() - start
      t.error(err)
      if (total === 0) t.fail('should respond with msgs')
      if (results.length !== 1) t.fail('should respond with 1 msg')
      if (!results[0].value.content.text.includes('LATESTMSG'))
        t.fail('should have LATESTMSG')
      t.pass(`duration: ${duration}ms`)
      fs.appendFileSync(reportPath, `- Initial indexing: ${duration}ms\n`)
      sbot.close(() => {
        t.end()
      })
    })
  )
})
