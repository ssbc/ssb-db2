// SPDX-FileCopyrightText: 2022 Andre 'Staltz' Medeiros <contact@staltz.com>
//
// SPDX-License-Identifier: CC0-1.0

const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const pull = require('pull-stream')
const pify = require('util').promisify
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const generateFixture = require('ssb-fixtures')
const fs = require('fs')
const { where, live, author, toPromise, toPullStream } = require('../operators')
const mentions = require('../operators/full-mentions')

const dir = '/tmp/ssb-db2-delete-feed-compact'

rimraf.sync(dir)
mkdirp.sync(dir)

const TOTAL = 100

test('generate fixture with flumelog-offset', (t) => {
  generateFixture({
    outputDir: dir,
    seed: 'migrate',
    messages: TOTAL,
    authors: 5,
    slim: true,
    followGraph: true,
  }).then(() => {
    t.true(
      fs.existsSync(path.join(dir, 'flume', 'log.offset')),
      'log.offset was created'
    )
    t.end()
  })
})

test('delete a feed and then compact', async (t) => {
  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .use(require('../full-mentions'))
    .call(null, {
      keys,
      path: dir,
      db2: { dangerouslyKillFlumeWhenMigrated: true },
    })

  await new Promise((resolve, reject) => {
    sbot.db2migrate.start()
    pull(
      sbot.db2migrate.progress(),
      pull.take(1),
      pull.collect((err) => {
        if (err) reject(err)
        else resolve()
      })
    )
  })

  let gotSpecialMention = false
  pull(
    sbot.db.query(where(mentions(sbot.id)), live(), toPullStream()),
    pull.drain((msg) => {
      if (msg.value.content.special) {
        if (gotSpecialMention) {
          t.fail('got more than one special mention')
        } else {
          gotSpecialMention = true
        }
      } else {
        t.fail('should not get any live mentions during reindexing')
      }
    })
  )

  const badPerson = '@a5URnr0bEtVFmNxQu/JwQ5JsfEl+HAPz5ethL+I1CCA=.ed25519'
  t.notEqual(sbot.id, badPerson, 'I am not the bad person')

  await new Promise((resolve) => {
    sbot.db.getStatus()((stats) => {
      if (stats.log > 0 && stats.progress === 1) {
        resolve()
        return false // abort listening to status
      }
    })
  })
  t.pass('status updated')

  const msgsBad1 = await sbot.db.query(where(author(badPerson)), toPromise())
  t.equals(msgsBad1.length, 18, 'bad person has published some messages')

  await pify(sbot.db.deleteFeed)(badPerson)
  t.pass('deleted bad person from my log')

  await pify(sbot.db.compact)()
  t.pass('compacted')

  await new Promise((resolve) => {
    sbot.db.getStatus()((stats) => {
      if (stats.log > 0 && stats.progress === 1) {
        resolve()
        return false // abort listening to status
      }
    })
  })
  t.pass('status updated')

  const msgsBad2 = await sbot.db.query(where(author(badPerson)), toPromise())
  t.equals(msgsBad2.length, 0, 'zero messages by bad person')

  await pify(sbot.db.publishAs)(ssbKeys.generate(), {
    type: 'post',
    text: 'Hi',
    special: 'yes',
    mentions: [{ link: sbot.id }],
  })
  t.pass('published new mention')

  await pify(setTimeout)(1000)
  t.true(gotSpecialMention, 'got special mention')

  await pify(sbot.close)(true)
  t.end()
})
