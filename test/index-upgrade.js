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
const bipf = require('bipf')

const Plugin = require('../indexes/plugin')

const dir = '/tmp/ssb-db2-index-upgrade'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const B_KEY = Buffer.from('key')

class IndexTestV1 extends Plugin {
  constructor(log, dir) {
    super(log, dir, 'indextest', 1)
  }

  processRecord(record, seq) {
    const buf = record.value
    const pKey = bipf.seekKey(buf, 0, B_KEY)
    const key = bipf.decode(buf, pKey)
    this.batch.push({
      type: 'put',
      key,
      value: 1,
    })
  }

  getValue(key, cb) {
    this.level.get(key, cb)
  }
}

class IndexTestV2 extends Plugin {
  constructor(log, dir) {
    super(log, dir, 'indextest', 2)
  }

  processRecord(record, seq) {
    const buf = record.value
    const pKey = bipf.seekKey(buf, 0, B_KEY)
    const key = bipf.decode(buf, pKey)
    this.batch.push({
      type: 'put',
      key,
      value: 2,
    })
  }

  getValue(key, cb) {
    this.level.get(key, cb)
  }
}

let sbot = SecretStack({ appKey: caps.shs }).use(require('../')).call(null, {
  keys,
  path: dir,
})
sbot.db.registerIndex(IndexTestV1)

let msgKey

test('1 index first', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  sbot.db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    msgKey = postMsg.key

    sbot.db.onDrain('indextest', () => {
      sbot.db.getIndex('indextest').getValue(msgKey, (err, value) => {
        t.error(err, 'no err')
        t.equal(value, '1', 'correct initial value')
        sbot.close(t.end)
      })
    })
  })
})

test('second index', (t) => {
  sbot = SecretStack({ appKey: caps.shs }).use(require('../')).call(null, {
    keys,
    path: dir,
  })
  sbot.db.registerIndex(IndexTestV2)

  sbot.db.onDrain('indextest', () => {
    sbot.db.getIndex('indextest').getValue(msgKey, (err, value) => {
      t.error(err, 'no err')
      t.equal(value, '2', 'reindexes on version upgrade')
      sbot.close(t.end)
    })
  })
})
