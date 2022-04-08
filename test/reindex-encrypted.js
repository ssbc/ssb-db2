// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const pify = require('util').promisify
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')

const { and, where, author, type, toPromise } = require('../operators')
const fullMentions = require('../operators/full-mentions')

test('box2 group reindex larger', async (t) => {
  // Create group keys
  const groupKey = Buffer.from(
    '30720d8f9cbf37f6d7062826f6decac93e308060a8aaaa77e6a4747f40ee1a76',
    'hex'
  )
  const groupId = 'group1.8K-group'

  const groupKey2 = Buffer.from(
    '40720d8f9cbf37f6d7062826f6decac93e308060a8aaaa77e6a4747f40ee1a76',
    'hex'
  )
  const groupId2 = 'group2.8K-group'

  // Setup Alice
  const dirAlice = '/tmp/ssb-db2-box2-group-reindex2-alice'
  rimraf.sync(dirAlice)
  mkdirp.sync(dirAlice)
  const keysAlice = ssbKeys.loadOrCreateSync(path.join(dirAlice, 'secret'))
  const alice = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .use(require('ssb-db2-box2'))
    .call(null, {
      keys: keysAlice,
      path: dirAlice,
    })
  alice.box2.addGroupKey(groupId, groupKey)
  alice.box2.addGroupKey(groupId2, groupKey2)
  alice.box2.registerIsGroup((recp) => recp.endsWith('8K-group'))
  alice.box2.setReady()

  // Setup Bob
  const dirBob = '/tmp/ssb-db2-box2-group-reindex2-bob'
  rimraf.sync(dirBob)
  mkdirp.sync(dirBob)
  const keysBob = ssbKeys.loadOrCreateSync(path.join(dirBob, 'secret'))
  const bob = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .use(require('../about-self'))
    .use(require('../full-mentions'))
    .use(require('ssb-db2-box2'))
    .call(null, {
      keys: keysBob,
      path: dirBob,
    })
  bob.box2.registerIsGroup((recp) => recp.endsWith('8K-group'))
  bob.box2.setReady()

  // Alice publishes 5 messages, some of them box2
  let content0 = { type: 'about', text: 'not super secret1' }
  let content1 = {
    type: 'post',
    text: 'super secret2',
    mentions: [{ link: bob.id }],
    recps: [groupId2],
  }
  let content2 = { type: 'wierd' }
  let content3 = { type: 'about', text: 'super secret3', recps: [groupId] }
  let content4 = { type: 'post', text: 'super secret4', recps: [groupId] }

  const msg0 = await pify(alice.db.publish)(content0)
  const msg1 = await pify(alice.db.publish)(content1)
  const msg2 = await pify(alice.db.publish)(content2)
  const msg3 = await pify(alice.db.publish)(content3)
  const msg4 = await pify(alice.db.publish)(content4)

  t.false(msg0.value.content.endsWith('.box2'), 'public')
  t.true(msg1.value.content.endsWith('.box2'), 'box2 encoded')
  t.false(msg2.value.content.endsWith('.box2'), 'public')
  t.true(msg3.value.content.endsWith('.box2'), 'box2 encoded')
  t.true(msg4.value.content.endsWith('.box2'), 'box2 encoded')

  // First, Bob gets 2 messages and indexes those
  await pify(bob.db.add)(msg0.value)
  await pify(bob.db.add)(msg1.value)

  const results1 = await bob.db.query(
    where(and(author(alice.id), type('about'))),
    toPromise()
  )
  t.equal(results1.length, 1)
  t.equal(results1[0].value.content.text, 'not super secret1')

  // Then, Bob gets the remaining 3 messages
  await pify(bob.db.add)(msg2.value)
  await pify(bob.db.add)(msg3.value)
  await pify(bob.db.add)(msg4.value)

  // Bob joins group 1 and is able to decrypt some messages
  bob.box2.addGroupKey(groupId, groupKey)

  await pify(bob.db.reindexEncrypted)()

  const results2 = await bob.db.query(
    where(and(author(alice.id), type('post'))),
    toPromise()
  )
  t.equal(results2.length, 1)
  t.equal(results2[0].value.content.text, 'super secret4')

  // Bob doesn't get any results from a leveldb query on a msg in group 2
  const results3 = await bob.db.query(
    where(and(author(alice.id), fullMentions(bob.id))),
    toPromise()
  )
  t.equal(results3.length, 0)

  // Bob joins group 2 and is able to decrypt some messages
  bob.box2.addGroupKey(groupId2, groupKey2)

  await pify(bob.db.reindexEncrypted)()

  const results4 = await bob.db.query(
    where(and(author(alice.id), type('post'))),
    toPromise()
  )
  t.equal(results4.length, 2)
  t.equal(results4[0].value.content.text, 'super secret2')
  t.equal(results4[1].value.content.text, 'super secret4')

  const results5 = await bob.db.query(
    where(and(author(alice.id), type('about'))),
    toPromise()
  )
  t.equal(results5.length, 2)
  t.equal(results5[0].value.content.text, 'not super secret1')
  t.equal(results5[1].value.content.text, 'super secret3')

  // Bob get results from a leveldb query on a msg in group 2
  const results6 = await bob.db.query(where(fullMentions(bob.id)), toPromise())
  t.equal(results6.length, 1)
  t.equal(results6[0].value.content.text, 'super secret2')

  await Promise.all([pify(alice.close)(true), pify(bob.close)(true)])
  t.end()
})
