// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const test = require('tape')
const ssbKeys = require('ssb-keys')
const pify = require('util').promisify
const pull = require('pull-stream')
const push = require('push-stream')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')

const { and, where, author, type, toPromise } = require('../operators')
const fullMentions = require('../operators/full-mentions')

test('box2 group reindex larger', async (t) => {
  // Create group keys
  const groupKey1 = Buffer.from(
    '30720d8f9cbf37f6d7062826f6decac93e308060a8aaaa77e6a4747f40ee1a76',
    'hex'
  )
  const groupId1 = '%Aihvp+fMdt5CihjbOY6eZc0qCe0eKsrN2wfgXV2E3PM=.cloaked'

  const groupKey2 = Buffer.from(
    '40720d8f9cbf37f6d7062826f6decac93e308060a8aaaa77e6a4747f40ee1a76',
    'hex'
  )
  const groupId2 = '%Bihvp+fMdt5CihjbOY6eZc0qCe0eKsrN2wfgXV2E3PM=.cloaked'

  // Setup Alice
  const dirAlice = '/tmp/ssb-db2-box2-group-reindex2-alice'
  rimraf.sync(dirAlice)
  mkdirp.sync(dirAlice)
  const keysAlice = ssbKeys.generate(null, 'alice')
  const alice = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, {
      keys: keysAlice,
      path: dirAlice,
    })
  alice.box2.addGroupInfo(groupId1, { key: groupKey1 })
  alice.box2.addGroupInfo(groupId2, { key: groupKey2 })

  // Setup Bob
  const dirBob = '/tmp/ssb-db2-box2-group-reindex2-bob'
  rimraf.sync(dirBob)
  mkdirp.sync(dirBob)
  const keysBob = ssbKeys.generate(null, 'bob')
  const bob = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .use(require('../full-mentions'))
    .call(null, {
      keys: keysBob,
      path: dirBob,
    })

  // Alice publishes 5 messages, some of them box2
  let opts1 = {
    keys: keysAlice,
    content: { type: 'about', text: 'not super secret1' },
  }
  let opts2 = {
    keys: keysAlice,
    content: {
      type: 'post',
      text: 'super secret2',
      mentions: [{ link: bob.id }],
    },
    recps: [groupId2],
    encryptionFormat: 'box2',
  }
  let opts3 = {
    content: { type: 'weird' },
    keys: keysAlice,
  }
  let opts4 = {
    content: { type: 'about', text: 'super secret4', recps: [groupId1] },
    keys: keysAlice,
    encryptionFormat: 'box2',
  }
  let opts5 = {
    content: { type: 'post', text: 'super secret5', recps: [groupId1] },
    keys: keysAlice,
    encryptionFormat: 'box2',
  }

  const msg1 = await pify(alice.db.create)(opts1)
  const msg2 = await pify(alice.db.create)(opts2)
  const msg3 = await pify(alice.db.create)(opts3)
  const msg4 = await pify(alice.db.create)(opts4)
  const msg5 = await pify(alice.db.create)(opts5)
  t.pass('alice published 5 messages')

  t.notEqual(typeof msg1.value.content, 'string', 'msg1 is public about')
  t.true(msg2.value.content.endsWith('.box2'), 'msg2 is group2-box2 post')
  t.notEqual(typeof msg3.value.content, 'string', 'msg3 is public weird')
  t.true(msg4.value.content.endsWith('.box2'), 'msg4 is group1-box2 about')
  t.true(msg5.value.content.endsWith('.box2'), 'msg5 is group1-box2 post')

  // First, Bob gets 2 messages and indexes those
  await pify(bob.db.add)(msg1.value)
  await pify(bob.db.add)(msg2.value)
  t.pass('bob added msg1 and msg2')

  const results1 = await bob.db.query(
    where(and(author(alice.id), type('about'))),
    toPromise()
  )
  t.equal(results1.length, 1, 'only one (public) about indexed')
  t.equal(results1[0].value.content.text, 'not super secret1', 'content is ok')

  // Then, Bob gets the remaining 3 messages
  await pify(bob.db.add)(msg3.value)
  await pify(bob.db.add)(msg4.value)
  await pify(bob.db.add)(msg5.value)
  t.pass('bob added msg3, msg4 and msg5')

  const results2 = await bob.db.query(
    where(and(author(alice.id), type('post'))),
    toPromise()
  )
  t.equal(results2.length, 0, 'no (public) posts indexed')

  // Bob joins group 1 and is able to decrypt some messages
  bob.box2.addGroupInfo(groupId1, { key: groupKey1 })
  t.pass('bob joined group 1')

  await pify(bob.db.reindexEncrypted)()
  t.pass('bob reindexed encrypted messages')

  const results3 = await bob.db.query(
    where(and(author(alice.id), type('post'))),
    toPromise()
  )
  t.equal(results3.length, 1, 'one group1-box2 post indexed')
  t.equal(results3[0].value.content.text, 'super secret5')

  // Bob doesn't get any results from a leveldb query on a msg in group 2
  const results4 = await bob.db.query(
    where(and(author(alice.id), fullMentions(bob.id))),
    toPromise()
  )
  t.equal(results4.length, 0, 'no results from group2 msgs')

  // Bob joins group 2 and is able to decrypt some messages
  bob.box2.addGroupInfo(groupId2, { key: groupKey2 })
  t.pass('bob joined group 2')

  await pify(bob.db.reindexEncrypted)()
  t.pass('bob reindexed encrypted messages')

  const results5 = await bob.db.query(
    where(and(author(alice.id), type('post'))),
    toPromise()
  )
  t.equal(results5.length, 2, 'two box2 post indexed')
  t.equal(results5[0].value.content.text, 'super secret2', 'content is ok')
  t.equal(results5[1].value.content.text, 'super secret5', 'content is ok')

  const results6 = await bob.db.query(
    where(and(author(alice.id), type('about'))),
    toPromise()
  )
  t.equal(results6.length, 2, 'two box2 about indexed')
  t.equal(results6[0].value.content.text, 'not super secret1', 'content is ok')
  t.equal(results6[1].value.content.text, 'super secret4', 'content is ok')

  // Bob get results from a leveldb query on a msg in group 2
  const results7 = await bob.db.query(where(fullMentions(bob.id)), toPromise())
  t.equal(results7.length, 1, 'one result from group2 msgs')
  t.equal(results7[0].value.content.text, 'super secret2', 'content is ok')

  await Promise.all([pify(alice.close)(true), pify(bob.close)(true)])
  t.end()
})

test.only('reindexEncrypted is crash resistant', async (t) => {
  // Create group keys
  const groupKey1 = Buffer.from(
    '30720d8f9cbf37f6d7062826f6decac93e308060a8aaaa77e6a4747f40ee1a76',
    'hex'
  )
  const groupId1 = '%Aihvp+fMdt5CihjbOY6eZc0qCe0eKsrN2wfgXV2E3PM=.cloaked'

  // Setup Alice
  const dirAlice = '/tmp/ssb-db2-box2-group-reindex2-alice'
  rimraf.sync(dirAlice)
  mkdirp.sync(dirAlice)
  const keysAlice = ssbKeys.generate(null, 'alice')
  const alice = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, {
      keys: keysAlice,
      path: dirAlice,
    })
  alice.box2.addGroupInfo(groupId1, { key: groupKey1 })

  // Setup Bob
  const dirBob = '/tmp/ssb-db2-box2-group-reindex2-bob'
  rimraf.sync(dirBob)
  mkdirp.sync(dirBob)
  const keysBob = ssbKeys.generate(null, 'bob')
  let bob = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .use(require('../full-mentions'))
    .call(null, {
      keys: keysBob,
      path: dirBob,
    })

  // Alice publishes 5 messages, some of them box2
  let opts1 = {
    keys: keysAlice,
    content: { type: 'about', text: 'not super secret1' },
  }
  let opts2 = {
    keys: keysAlice,
    content: {
      type: 'post',
      text: 'super secret2',
      mentions: [{ link: bob.id }],
    },
    recps: [groupId1],
    encryptionFormat: 'box2',
  }
  let opts3 = {
    content: { type: 'weird' },
    keys: keysAlice,
  }
  let opts4 = {
    content: { type: 'about', text: 'super secret4', recps: [groupId1] },
    keys: keysAlice,
    encryptionFormat: 'box2',
  }
  let opts5 = {
    content: {
      type: 'post',
      text: 'super secret5',
      mentions: [{ link: bob.id }],
      recps: [groupId1],
    },
    keys: keysAlice,
    encryptionFormat: 'box2',
  }

  const msg1 = await pify(alice.db.create)(opts1)
  const msg2 = await pify(alice.db.create)(opts2)
  const msg3 = await pify(alice.db.create)(opts3)
  const msg4 = await pify(alice.db.create)(opts4)
  const msg5 = await pify(alice.db.create)(opts5)
  t.pass('alice published 5 messages')

  t.notEqual(typeof msg1.value.content, 'string', 'msg1 is public about')
  t.true(msg2.value.content.endsWith('.box2'), 'msg2 is group1-box2 post')
  t.notEqual(typeof msg3.value.content, 'string', 'msg3 is public weird')
  t.true(msg4.value.content.endsWith('.box2'), 'msg4 is group1-box2 about')
  t.true(msg5.value.content.endsWith('.box2'), 'msg5 is group1-box2 post')

  // First, Bob gets 2 messages and indexes those
  await pify(bob.db.add)(msg1.value)
  await pify(bob.db.add)(msg2.value)
  await pify(bob.db.add)(msg3.value)
  await pify(bob.db.add)(msg4.value)
  await pify(bob.db.add)(msg5.value)
  t.pass('bob added all 5 messages')

  // Get offsets of those messages
  const bobLog = bob.db.getLog()
  const offsets = await new Promise((resolve) => {
    bobLog.stream({ offsets: true, values: false }).pipe(
      push.collect((err, ary) => {
        t.error(err, 'no error when streaming bobLog')
        resolve(ary)
      })
    )
  })

  const results1 = await bob.db.query(
    where(and(author(alice.id), fullMentions(bob.id))),
    toPromise()
  )
  t.equal(results1.length, 0, 'bob has no box2 mentions from alice')

  // Bob joins group 1 and is able to decrypt some messages
  bob.box2.addGroupInfo(groupId1, { key: groupKey1 })
  t.pass('bob joined group 1')

  // Wait for private indexes to be saved to disk
  await pify(setTimeout)(2000)

  // Hack log.get to make it crash on the 2nd box2 msg
  const originalGet = bobLog.get
  bobLog.get = (offset, cb) => {
    if (offset === offsets[3]) {
      // offsets[3] is the 4th msg
      return cb(new Error('simulated crash'))
    } else {
      originalGet.call(bobLog, offset, cb)
    }
  }

  try {
    await pify(bob.db.reindexEncrypted)()
    t.fail('reindexEncrypted should have thrown')
  } catch (err) {
    t.true(err.cause.message.includes('simulated crash'), 'simulated crash')
  }

  await Promise.all([pify(alice.close)(true), pify(bob.close)(true)])

  bob = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .use(require('../full-mentions'))
    .call(null, {
      keys: keysBob,
      path: dirBob,
    })

  const streamed = []
  pull(
    bob.db.reindexed(),
    pull.drain((msg) => {
      streamed.push(msg.value.content.text)
    })
  )

  await pify(setTimeout)(4000)

  const results2 = await bob.db.query(
    where(and(author(alice.id), fullMentions(bob.id))),
    toPromise()
  )
  t.equal(results2.length, 2, 'bob has two box2 mentions from alice')

  t.deepEquals(
    streamed,
    ['super secret2', 'super secret4', 'super secret5'],
    'reindexed stream correct'
  )

  await pify(bob.close)(true)
  t.end()
})
