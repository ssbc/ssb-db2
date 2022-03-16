// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const validate = require('ssb-validate')
const pull = require('pull-stream')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')

const { author, type } = require('../operators')

const dir = '/tmp/ssb-db2-delete'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const sbot = SecretStack({ appKey: caps.shs })
  .use(require('../'))
  .use(require('../compat/ebt'))
  .call(null, {
    keys,
    path: dir,
  })
const db = sbot.db

test('index, delete, query', (t) => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.getLog().onDrain(() => {
      // create index
      db.getJITDB().all(
        author(keys.id),
        0,
        false,
        false,
        'declared',
        (err, results) => {
          t.error(err, 'no err')
          t.equal(results.length, 1, 'got msg')

          db.publish(post2, (err, post2Msg) => {
            t.error(err, 'no err')

            db.del(postMsg.key, (err) => {
              t.error(err, 'no err')

              db.getJITDB().all(
                type('post'),
                0,
                false,
                false,
                'declared',
                (err, results) => {
                  t.equal(results.length, 1, 'got msg')
                  t.equal(results[0].value.sequence, 2, 'got correct msg')
                  sbot.close(t.end)
                }
              )
            })
          })
        }
      )
    })
  })
})
