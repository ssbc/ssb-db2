// SPDX-FileCopyrightText: 2022 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const test = require('tape')
const ssbKeys = require('ssb-keys')
const fs = require('fs')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const pify = require('util').promisify
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const {resetLevelPath, resetPrivatePath, reindexJitPath} = require('../defaults')

const dir = '/tmp/ssb-db2-compaction-resume'

rimraf.sync(dir)
mkdirp.sync(dir)

test('compaction resumes automatically after a crash', async (t) => {
  t.timeoutAfter(20e3)

  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  let sbot = SecretStack({ appKey: caps.shs }).use(require('../')).call(null, {
    keys,
    path: dir,
  })

  const TOTAL = 1000
  const msgKeys = []
  for (let i = 0; i < TOTAL; i += 1) {
    const msg = await pify(sbot.db.publish)({ type: 'post', text: `hi ${i}` })
    msgKeys.push(msg.key)
  }
  t.pass('published messages')

  await pify(sbot.db.onDrain)('keys')
  const oldLogSize = sbot.db.getStatus().value.log

  const keysIndex = sbot.db.getIndex('keys')
  const seq3 = await pify(keysIndex.getSeq.bind(keysIndex))(msgKeys[3])
  t.equals(seq3, 3, 'seq 3 for msg #3')

  for (let i = 0; i < TOTAL; i += 2) {
    await pify(sbot.db.del)(msgKeys[i])
  }
  t.pass('deleted messages')

  await pify(sbot.close)(true)
  t.pass('closed sbot')

  fs.closeSync(fs.openSync(path.join(dir, 'db2', 'log.bipf.compaction'), 'w'))
  fs.closeSync(fs.openSync(resetLevelPath(dir), 'w'))
  fs.closeSync(fs.openSync(resetPrivatePath(dir), 'w'))
  fs.closeSync(fs.openSync(reindexJitPath(dir), 'w'))
  t.pass('pretend that compaction was in progress')

  sbot = SecretStack({ appKey: caps.shs }).use(require('../')).call(null, {
    keys,
    path: dir,
  })

  let newLogSize = 0
  let done = false
  sbot.db.getStatus()((stats) => {
    if (!stats.log) return
    if (stats.log > oldLogSize * 0.6) return
    if (newLogSize) return
    if (stats.progress !== 1) return

    newLogSize = stats.log
    done = true
  })

  await new Promise((resolve) => {
    const interval = setInterval(() => {
      if (done) {
        clearInterval(interval)
        resolve()
      }
    }, 200)
  })
  t.pass('compaction started and ended automatically')

  try {
    const keysIndex2 = sbot.db.getIndex('keys')
    const seq3after = await pify(keysIndex2.getSeq.bind(keysIndex2))(msgKeys[3])
    t.equals(seq3after, 1, 'seq 1 for msg #3 after reindexing')
  } catch (err) {
    console.log(err)
  }

  t.notEquals(oldLogSize, 0, 'old log size is ' + oldLogSize)
  t.notEquals(newLogSize, 0, 'new log size is ' + newLogSize)
  t.true(newLogSize < oldLogSize * 0.6, 'at most 0.6x smaller')
  t.true(newLogSize > oldLogSize * 0.4, 'at least 0.4x smaller')

  await pify(sbot.close)(true)
  t.end()
})
