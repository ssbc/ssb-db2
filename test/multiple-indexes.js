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

const { where, toCallback } = require('../operators')
const mentions = require('../operators/full-mentions')

const dir = '/tmp/ssb-db2-multiple'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

let sbot = SecretStack({ appKey: caps.shs })
  .use(require('../'))
  .use(require('../full-mentions'))
  .call(null, {
    keys,
    path: dir,
  })
let db = sbot.db

test('1 index first', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.onDrain('fullMentions', () => {
      sbot.close(true, () => t.end())
    })
  })
})

test('second index', (t) => {
  sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .use(require('../compat/ebt'))
    .use(require('../full-mentions'))
    .call(null, {
      keys,
      path: dir,
    })
  let db = sbot.db

  const feedId = '@abc'
  const mentionFeed = {
    type: 'post',
    text: 'Hello @abc',
    mentions: [{ link: feedId }],
  }

  db.publish(mentionFeed, (err) => {
    t.error(err, 'no err')

    db.query(
      where(mentions(feedId)),
      toCallback((err, results) => {
        t.error(err, 'no err')
        t.equal(results.length, 1)
        t.equal(results[0].value.content.text, mentionFeed.text)
        sbot.close(true, () => t.end())
      })
    )
  })
})
