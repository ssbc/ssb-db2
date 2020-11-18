const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const generateFixture = require('ssb-fixtures')
const fs = require('fs')
const { toCallback } = require('../operators')

const dir = '/tmp/ssb-db2-migration'

rimraf.sync(dir)
mkdirp.sync(dir)

test('generate fixture with flumelog-offset', (t) => {
  generateFixture({
    outputDir: dir,
    seed: 'migration',
    messages: 2,
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

  // FIXME: replace setTimeout with migration progress events
  setTimeout(() => {
    t.true(fs.existsSync(path.join(dir, 'db2', 'log.bipf')), 'migration done')
    sbot.db.onDrain(() => {
      sbot.db.query(
        toCallback((err1, msgs) => {
          t.error(err1, 'no err')
          t.equal(msgs.length, 2)
          sbot.close(() => {
            t.end()
          })
        })
      )
    })
  }, 1000)
})

test('migration keeps new log synced with old log being updated', (t) => {
  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('ssb-db'))
    .use(require('../index'))
    .use(require('../migration'))
    .call(null, { keys, path: dir })

  // FIXME: replace setTimeout with migration progress events
  setTimeout(() => {
    t.true(fs.existsSync(path.join(dir, 'db2', 'log.bipf')), 'migration done')
    sbot.db.onDrain(() => {
      sbot.db.query(
        toCallback((err1, msgs) => {
          t.error(err1, '1st query suceeded')
          t.equal(msgs.length, 2, '2 msgs')

          sbot.publish({ type: 'post', text: 'Extra post' }, (err2, posted) => {
            t.error(err2, 'publish suceeded')
            t.equals(posted.value.content.type, 'post', 'msg posted')

            // FIXME: replace setTimeout with migration progress events
            setTimeout(() => {
              sbot.db.query(
                toCallback((err3, msgs2) => {
                  t.error(err3, '2nd query suceeded')
                  t.equal(msgs2.length, 3, '3 msgs')
                  sbot.close(() => {
                    t.end()
                  })
                })
              )
            }, 1000)
          })
        })
      )
    })
  }, 1000)
})
