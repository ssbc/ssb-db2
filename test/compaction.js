// SPDX-FileCopyrightText: 2022 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const pify = require('util').promisify
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const { where, key, toCallback } = require('../operators')
const { onceWhen } = require('../utils')

test('compaction fills holes and reindexes', async (t) => {
  t.timeoutAfter(20e3)

  const dir = '/tmp/ssb-db2-compaction'

  rimraf.sync(dir)
  mkdirp.sync(dir)

  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, {
      keys: ssbKeys.loadOrCreateSync(path.join(dir, 'secret')),
      path: dir,
    })

  const TOTAL = 1000
  const msgKeys = []
  console.time('publish')
  for (let i = 0; i < TOTAL; i += 1) {
    const msg = await pify(sbot.db.publish)({ type: 'post', text: `hi ${i}` })
    msgKeys.push(msg.key)
  }
  t.pass('published messages')
  console.timeEnd('publish')

  await pify(sbot.db.onDrain)()
  const oldLogSize = sbot.db.getStatus().value.log

  const msg3 = await pify(sbot.db.getMsg)(msgKeys[3])
  t.equals(msg3.value.content.text, 'hi 3')

  const keysIndex = sbot.db.getIndex('keys')
  const seq3 = await pify(keysIndex.getSeq.bind(keysIndex))(msgKeys[3])
  t.equals(seq3, 3, 'seq 3 for msg #3')

  console.time('delete')
  for (let i = 0; i < TOTAL; i += 2) {
    await pify(sbot.db.del)(msgKeys[i])
  }
  console.timeEnd('delete')
  await pify(sbot.db.getLog().onDeletesFlushed)()
  t.pass('deleted messages')

  let newLogSize = 0
  let done = false
  sbot.db.getStatus()((stats) => {
    if (!stats.log) return
    if (stats.log > oldLogSize * 0.6) return
    if (newLogSize) return
    if (stats.progress !== 1) return

    newLogSize = stats.log
    console.timeEnd('reindex')
    done = true
  })

  console.time('compact')
  await pify(sbot.db.compact)()
  console.timeEnd('compact')
  console.time('reindex')

  await new Promise((resolve) => {
    const interval = setInterval(() => {
      if (done) {
        clearInterval(interval)
        resolve()
      }
    }, 200)
  })

  const seq3after = await pify(keysIndex.getSeq.bind(keysIndex))(msgKeys[3])
  t.equals(seq3after, 1, 'seq 1 for msg #3 after reindexing')

  t.notEquals(oldLogSize, 0, 'old log size is ' + oldLogSize)
  t.notEquals(newLogSize, 0, 'new log size is ' + newLogSize)
  t.true(newLogSize < oldLogSize * 0.6, 'at most 0.6x smaller')
  t.true(newLogSize > oldLogSize * 0.4, 'at least 0.4x smaller')

  await pify(sbot.close)(true)
  t.end()
})

test('queries are queued if compaction is in progress', async (t) => {
  t.timeoutAfter(20e3)

  const dir = '/tmp/ssb-db2-compaction2'

  rimraf.sync(dir)
  mkdirp.sync(dir)

  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, {
      keys: ssbKeys.loadOrCreateSync(path.join(dir, 'secret')),
      path: dir,
    })

  const TOTAL = 1000
  const msgKeys = []
  for (let i = 0; i < TOTAL; i += 1) {
    const msg = await pify(sbot.db.publish)({ type: 'post', text: `hi ${i}` })
    msgKeys.push(msg.key)
  }
  t.pass('published messages')

  await pify(sbot.db.onDrain)()

  for (let i = 0; i < TOTAL; i += 2) {
    await pify(sbot.db.del)(msgKeys[i])
  }
  t.pass('deleted messages')

  let compactDoneAt = 0
  let queryDoneAt = 0
  await new Promise((resolve) => {
    sbot.db.compact((err) => {
      t.error(err, 'no error')
      compactDoneAt = Date.now()
      if (queryDoneAt > 0) resolve()
    })

    onceWhen(
      sbot.db.getLog().compactionProgress,
      (stat) => stat.done === false,
      () => {
        sbot.db.query(
          where(key(msgKeys[3])),
          toCallback((err, msgs) => {
            t.error(err, 'no error')
            t.equals(msgs.length, 1)
            t.equals(msgs[0].value.content.text, 'hi 3')
            queryDoneAt = Date.now()
            if (compactDoneAt > 0) resolve()
          })
        )
      }
    )
  })
  t.true(compactDoneAt < queryDoneAt, 'compaction done before query')

  await pify(sbot.close)(true)
  t.end()
})

test('post-compaction reindex resets state in memory too', async (t) => {
  t.timeoutAfter(20e3)

  const dir = '/tmp/ssb-db2-compaction3'

  rimraf.sync(dir)
  mkdirp.sync(dir)

  const author = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .use(require('../about-self'))
    .call(null, {
      keys: author,
      path: dir,
    })

  const msg1 = await pify(sbot.db.publish)({
    type: 'about',
    about: author.id,
    name: 'Alice',
  })
  t.pass('published name about')
  const msg2 = await pify(sbot.db.publish)({
    type: 'about',
    about: author.id,
    description: 'In Wonderland',
  })
  t.pass('published description about')

  await pify(sbot.db.onDrain)('aboutSelf')

  const profileBefore = sbot.db.getIndex('aboutSelf').getProfile(author.id)
  t.equal(profileBefore.name, 'Alice')
  t.equal(profileBefore.description, 'In Wonderland')

  await pify(sbot.db.del)(msg2.key)
  t.pass('deleted description about')

  const offsetBefore = sbot.db.getStatus().value.log
  t.true(offsetBefore > 0, 'log offset is > 0')
  t.equals(sbot.db.getStatus().value.indexes.base, offsetBefore, 'status for base index is latest offset')

  await pify(sbot.db.compact)()
  t.pass('compacted the log')

  t.equals(sbot.db.getStatus().value.indexes.base, -1, 'status for base index is -1')

  await pify(sbot.db.onDrain)('aboutSelf')

  t.equals(sbot.db.getStatus().value.indexes.base, offsetBefore, 'status for base index is latest offset')
  const profileAfter = sbot.db.getIndex('aboutSelf').getProfile(author.id)
  t.equal(profileAfter.name, 'Alice')
  t.notOk(profileAfter.description)

  await pify(sbot.close)(true)
  t.end()
})
