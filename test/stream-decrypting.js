// SPDX-FileCopyrightText: 2022 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const pull = require('pull-stream')
const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const pify = require('util').promisify
const SecretStack = require('secret-stack')
const bipf = require('bipf')
const caps = require('ssb-caps')
const Plugin = require('../indexes/plugin')
const { live, toPullStream } = require('../operators')

test('log.stream decrypting', async (t) => {
  t.timeoutAfter(20e3)

  const dir = '/tmp/ssb-db2-stream-decrypting'

  rimraf.sync(dir)
  mkdirp.sync(dir)

  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, {
      keys,
      path: dir,
    })

  // Purposefully slow down log.getBlock so that we can simulate race conditions
  const log = sbot.db.getLog()
  const originalGetBlock = log.getBlock
  log.getBlock = function getBlock(offset, cb) {
    // Speed up lookups at the end of the log
    const period = offset > log.since.value * 0.9 ? 0 : 200
    setTimeout(originalGetBlock, period, offset, cb)
  }

  let bingoCount = 0
  sbot.db.registerIndex(
    class TestPlugin extends Plugin {
      constructor(log, dir) {
        super(log, dir, 'testPlugin', 1)
      }

      processRecord(record, seq, pValue) {
        const buf = record.value
        const pValueContent = bipf.seekKey(buf, pValue, 'content')
        if (pValueContent < 0) return
        const pValueContentType = bipf.seekKey(buf, pValueContent, 'type')
        if (pValueContentType < 0) return
        const type = bipf.decode(buf, pValueContentType)
        if (type === 'bingo') {
          bingoCount += 1
        }
      }
    }
  )

  const TOTAL = 400
  const DELETES = 40
  const msgKeys = []
  for (let i = 0; i < TOTAL; i += 1) {
    const msg = await pify(sbot.db.publish)({ type: 'post', text: `hi ${i}` })
    msgKeys.push(msg.key)
  }
  t.pass('published messages')

  await pify(sbot.db.create)({
    keys,
    content: { type: 'bingo', text: 'private!' },
    recps: [sbot.id],
  })
  t.pass('published a private message')

  await pify(sbot.db.create)({
    keys,
    content: { type: 'post', text: 'hello' },
  })
  t.pass('published a public message')

  await pify(sbot.db.onDrain)('testPlugin')
  t.equal(bingoCount, 1, 'processed private record')

  let foundLatestMsg = false
  pull(
    sbot.db.query(live({ old: false }), toPullStream()),
    pull.drain((msg) => {
      if (msg.value.content.text === 'latest') {
        foundLatestMsg = true
      }
    })
  )

  for (let i = 0; i < DELETES; i += 1) {
    await pify(sbot.db.del)(msgKeys[i])
  }
  await pify(sbot.db.getLog().onDeletesFlushed)()
  t.pass('deleted ' + DELETES + ' messages')

  await pify(sbot.db.compact)()
  t.pass('compacted')

  await pify(sbot.db.create)({
    content: { type: 'post', text: 'latest' },
  })
  t.pass('published a new message')

  await pify(sbot.db.onDrain)('testPlugin')
  t.pass('reindexed')

  await new Promise((resolve) => {
    const interval = setInterval(() => {
      if (foundLatestMsg) {
        clearInterval(interval)
        resolve()
      }
    }, 200)
  })

  t.equal(bingoCount, 2, 're-processed private record')

  t.pass('lets close up')
  await pify(sbot.close)(true)
  t.end()
})
