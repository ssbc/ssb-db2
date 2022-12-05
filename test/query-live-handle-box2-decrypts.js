const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const ssbUri = require('ssb-uri2')
const pull = require('pull-stream')
const fs = require('fs')
const {
  where,
  type,
  live,
  toPullStream,
  toCallback
} = require('../operators')

const dir = '/tmp/ssb-db2-query-live-handle-box2-decrypts'
rimraf.sync(dir)
mkdirp.sync(dir)

const dir2 = '/tmp/ssb-db2-query-live-handle-box2-decrypts2'
rimraf.sync(dir2)
mkdirp.sync(dir2)

test('live query() contains decrypted box2 messages', (t) => {
  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, { keys, path: dir })

  const testkey = Buffer.from(
    '30720d8f9cbf37f6d7062826f6decac93e308060a8aaaa77e6a4747f40ee1a76',
    'hex'
  )
  const groupId = ssbUri.compose({
    type: 'identity',
    format: 'group',
    data: '-oaWWDs8g73EZFUMfW37R_ULtFEjwKN_DczvdYihjbU=',
  })
  sbot.box2.addGroupInfo(groupId, { key: testkey })

  const post = {
    feedFormat: 'classic',
    content: { type: 'post', text: 'Testing!' },
    recps: [groupId],
    encryptionFormat: 'box2',
  }

  sbot.db.create(post, (err, msgBoxed) => {
    t.error(err, 'no err')
    t.equal(typeof msgBoxed.value.content, 'string')
    t.true(msgBoxed.value.content.endsWith('.box2'), '.box2')

    const keys2 = ssbKeys.loadOrCreateSync(path.join(dir2, 'secret'))
    const sbot2 = SecretStack({ appKey: caps.shs })
      .use(require('../'))
      .call(null, { keys: keys2, path: dir2 })

    // setup live handler
    pull(
      sbot2.db.query(
        where(type('post')),
        live({ old: true }),
        toPullStream()
      ),
      pull.drain(
        (result) => {
          t.equal(result.value.content.text, 'Testing!')
          sbot.close(() => {
            sbot2.close(t.end)
          })
        }
      )
    )

    sbot2.db.add(msgBoxed.value, (err) => {
      t.error(err, 'no err')

      // make sure we have queries indexed before adding the key
      pull(
        sbot2.db.query(
          where(type('post')),
          toCallback((err, results) => {
            t.error(err, 'no err')
            t.equal(results.length, 0, 'no results')

            sbot2.box2.addGroupInfo(groupId, { key: testkey })
            sbot2.db.reindexEncrypted((err) => {
              t.error(err, 'no err')
              console.log("finished reindexing")
            })
          })
        )
      )
    })
  })
})
