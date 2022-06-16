// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const classic = require('ssb-classic/format')
const pull = require('pull-stream')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const bendyButt = require('ssb-bendy-butt')

const {
  where,
  author,
  authorIsBendyButtV1,
  type,
  toPullStream,
  toPromise,
} = require('../operators')

const dir = '/tmp/ssb-db2-basic'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

let sbot = SecretStack({ appKey: caps.shs })
  .use(require('../'))
  .use(require('../compat/ebt'))
  .call(null, {
    keys,
    path: dir,
  })
let db = sbot.db

test('onDrain not called after db closed', (t) => {
  sbot.close(() => {
    t.pass('closed sbot')

    db.onDrain(() => {
      t.fail('onDrain called after db closed')
    })

    setTimeout(() => {
      sbot = SecretStack({ appKey: caps.shs })
        .use(require('../'))
        .use(require('../compat/ebt'))
        .call(null, {
          keys,
          path: dir,
        })
      db = sbot.db
      t.pass('restarted sbot')
      t.end()
    }, 200)
  })
})

test('Base', (t) => {
  const posts = []
  for (var i = 0; i < 30; ++i) posts.push({ type: 'post', text: 'Testing!' })

  let j = 0

  pull(
    pull.values(posts),
    pull.asyncMap(db.publish),
    pull.asyncMap((postMsg, cb) => {
      if (j++ % 3 === 0) {
        sbot.getAtSequence([keys.id, j], (err, msg) => {
          t.error(err, 'no err')
          t.equal(msg.key, postMsg.key)
          cb(err)
        })
      } else cb()
    }),
    pull.collect((err) => {
      t.end()
    })
  )
})

test('get', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')
    t.equal(postMsg.value.content.text, post.text, 'text correct')

    db.get(postMsg.key, (err, getMsg) => {
      t.error(err, 'no err')
      t.deepEqual(postMsg.value, getMsg, 'msg value correct')
      t.end()
    })
  })
})

test('getStatus', (t) => {
  db.onDrain('ebt', () => {
    const stats = db.getStatus().value
    t.pass(JSON.stringify(stats))
    t.ok(stats.log)
    t.ok(stats.jit)
    t.ok(stats.indexes)
    t.true(stats['log'] > 0)
    t.equal(stats.jit['seq'], stats['log'])
    t.equal(stats.indexes['base'], stats['log'])
    t.equal(stats.indexes['ebt'], stats['log'])
    t.end()
  })
})

test('get missing key', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.get('%fake', (err, getMsg) => {
      t.equal(err.message, 'Msg %fake not found in leveldb index')
      t.end()
    })
  })
})

test('getMsg', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')
    t.equal(postMsg.value.content.text, post.text, 'text correct')

    db.getMsg(postMsg.key, (err, getMsg) => {
      t.error(err, 'no err')
      t.deepEqual(postMsg, getMsg, 'msg value correct')
      t.end()
    })
  })
})

test('delete single msg', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')
    t.equal(postMsg.value.content.text, post.text, 'text correct')

    db.get(postMsg.key, (err, getMsg) => {
      t.error(err, 'no err')
      t.equal(postMsg.value.content.text, getMsg.content.text, 'text correct')

      db.del(postMsg.key, (err) => {
        t.error(err, 'no err')

        db.get(postMsg.key, (err, msg) => {
          t.equal(msg, undefined, 'msg gone')
          t.end()
        })
      })
    })
  })
})

test('deleteFeed', (t) => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err) => {
    t.error(err, 'no err')

    db.publish(post2, (err) => {
      t.error(err, 'no err')

      db.getJITDB().all(
        author(keys.id),
        0,
        false,
        false,
        'declared',
        (err, results) => {
          t.error(err, 'no err')
          t.equal(results.length, 30 + 5, 'got both new messages')

          db.deleteFeed(keys.id, (err) => {
            t.error(err, 'no err')

            db.getJITDB().all(
              author(keys.id),
              0,
              false,
              false,
              'declared',
              (err, results) => {
                t.error(err, 'no err')
                t.equal(results.length, 0, 'gone')
                t.end()
              }
            )
          })
        }
      )
    })
  })
})

test('deleteFeed unknown', (t) => {
  db.deleteFeed(ssbKeys.generate().id, (err) => {
    t.error(err, 'no err')
    t.end()
  })
})

test('add three messages', (t) => {
  const rando = ssbKeys.generate()

  const msg1 = classic.newNativeMsg({
    keys: rando,
    content: { type: 'post', text: 'a' },
    previous: null,
    timestamp: Date.now() - 3,
    hmacKey: null,
  })
  const msg2 = classic.newNativeMsg({
    keys: rando,
    content: { type: 'post', text: 'b' },
    previous: { key: classic.getMsgId(msg1), value: msg1 },
    timestamp: Date.now() - 2,
    hmacKey: null,
  })
  const msg3 = classic.newNativeMsg({
    keys: rando,
    content: { type: 'post', text: 'c' },
    previous: { key: classic.getMsgId(msg2), value: msg2 },
    timestamp: Date.now() - 1,
    hmacKey: null,
  })

  const pickValue = (kvt) => kvt.value

  const msgVals = [msg1, msg2, msg3]
  pull(
    pull.values(msgVals),
    pull.asyncMap((msgVal, cb) => db.add(msgVal, cb)),
    pull.collect((err, kvts) => {
      t.error(err)
      t.deepEquals(kvts.map(pickValue), msgVals)
      db.onDrain(() => {
        pull(
          db.query(where(author(rando.id)), toPullStream()),
          pull.collect((err2, results) => {
            t.equals(results.length, 3)
            t.equal(results[0].value.content.text, 'a')
            t.equal(results[1].value.content.text, 'b')
            t.equal(results[2].value.content.text, 'c')
            t.end()
          })
        )
      })
    })
  )
})

test('add three messages in batch', (t) => {
  const rando = ssbKeys.generate()

  const msg4 = classic.newNativeMsg({
    keys: rando,
    content: { type: 'post', text: 'd' },
    previous: null,
    timestamp: Date.now() - 3,
    hmacKey: null,
  })
  const msg5 = classic.newNativeMsg({
    keys: rando,
    content: { type: 'post', text: 'e' },
    previous: { key: classic.getMsgId(msg4), value: msg4 },
    timestamp: Date.now() - 2,
    hmacKey: null,
  })
  const msg6 = classic.newNativeMsg({
    keys: rando,
    content: { type: 'post', text: 'f' },
    previous: { key: classic.getMsgId(msg5), value: msg5 },
    timestamp: Date.now() - 1,
    hmacKey: null,
  })

  const pickValue = (kvt) => kvt.value

  const msgVals = [msg4, msg5, msg6]
  db.addBatch(msgVals, (err, kvts) => {
    t.error(err, 'no err')
    t.equals(kvts.length, 3)
    t.deepEquals(kvts.map(pickValue), msgVals)
    t.end()
  })
})

test('multi batch', (t) => {
  const rando = ssbKeys.generate()

  const msg7 = classic.newNativeMsg({
    keys: rando,
    content: { type: 'post', text: 'g' },
    previous: null,
    timestamp: Date.now() - 4,
    hmacKey: null,
  })
  const msg8 = classic.newNativeMsg({
    keys: rando,
    content: { type: 'post', text: 'h' },
    previous: { key: classic.getMsgId(msg7), value: msg7 },
    timestamp: Date.now() - 3,
    hmacKey: null,
  })
  const msg9 = classic.newNativeMsg({
    keys: rando,
    content: { type: 'post', text: 'i' },
    previous: { key: classic.getMsgId(msg8), value: msg8 },
    timestamp: Date.now() - 2,
    hmacKey: null,
  })
  const msg10 = classic.newNativeMsg({
    keys: rando,
    content: { type: 'post', text: 'j' },
    previous: { key: classic.getMsgId(msg9), value: msg9 },
    timestamp: Date.now() - 1,
    hmacKey: null,
  })

  let done = 0

  const pickValue = (kvt) => kvt.value

  const msgVals = [msg7, msg8, msg9, msg10]

  const batch1 = msgVals.slice(0, 2)
  db.addBatch(batch1, (err, kvts) => {
    t.error(err, 'no err')
    t.equals(kvts.length, 2)
    t.deepEquals(kvts.map(pickValue), batch1)
    if (++done === 2) t.end()
  })

  const batch2 = msgVals.slice(2, 4)
  db.addBatch(batch2, (err, kvts) => {
    t.error(err, 'no err')
    t.equals(kvts.length, 2)
    t.deepEquals(kvts.map(pickValue), batch2)
    if (++done === 2) t.end()
  })
})

test('add transaction', (t) => {
  const rando = ssbKeys.generate()

  const msg11 = classic.newNativeMsg({
    keys: rando,
    content: { type: 'post', text: 'k' },
    previous: null,
    timestamp: Date.now() - 3,
    hmacKey: null,
  })
  const msg12 = classic.newNativeMsg({
    keys: rando,
    content: { type: 'post', text: 'l' },
    previous: { key: classic.getMsgId(msg11), value: msg11 },
    timestamp: Date.now() - 2,
    hmacKey: null,
  })
  const msg13 = classic.newNativeMsg({
    keys: rando,
    content: { type: 'post', text: 'm' },
    previous: { key: classic.getMsgId(msg12), value: msg12 },
    timestamp: Date.now() - 1,
    hmacKey: null,
  })

  const pickValue = (kvt) => kvt.value

  const msgVals = [msg11, msg12, msg13]
  db.addTransaction(msgVals, null, (err, kvts) => {
    t.error(err, 'no err')
    t.equals(kvts.length, 3)
    t.deepEquals(kvts.map(pickValue), msgVals)
    t.end()
  })
})

test('add transaction ooo', (t) => {
  const rando = ssbKeys.generate()

  const msg14 = classic.newNativeMsg({
    keys: rando,
    content: { type: 'post', text: 'o' },
    previous: null,
    timestamp: Date.now() - 3,
    hmacKey: null,
  })
  const msg15 = classic.newNativeMsg({
    keys: rando,
    content: { type: 'post', text: 'p' },
    previous: { key: classic.getMsgId(msg14), value: msg14 },
    timestamp: Date.now() - 2,
    hmacKey: null,
  })
  const msg16 = classic.newNativeMsg({
    keys: rando,
    content: { type: 'post', text: 'q' },
    previous: { key: classic.getMsgId(msg15), value: msg15 },
    timestamp: Date.now() - 1,
    hmacKey: null,
  })

  const msgVals = [msg14, msg15, msg16]
  db.addTransaction(msgVals.slice(0, 1), msgVals.slice(2, 3), (err, kvts) => {
    t.error(err, 'no err')
    t.equals(kvts.length, 2)
    t.equals(kvts[0].value, msgVals[0])
    t.equals(kvts[1].value, msgVals[2])

    // to test all the cases of the function, a more realistic example
    // would have the ooo messages from another feed entirely
    db.addTransaction(null, msgVals.slice(1, 2), (err, kvts) => {
      t.error(err, 'no err')
      t.equals(kvts.length, 1)
      t.equals(kvts[0].value, msgVals[1])
      t.end()
    })
  })
})

test('add some bendybutt-v1 messages', (t) => {
  const mfKeys = ssbKeys.generate(null, null, 'bendybutt-v1')
  const mainKeys = ssbKeys.generate()

  const bbmsg1 = bendyButt.encodeNew(
    {
      type: 'metafeed/add/existing',
      feedpurpose: 'main',
      subfeed: mainKeys.id,
      metafeed: mfKeys.id,
      tangles: {
        metafeed: {
          root: null,
          previous: null,
        },
      },
    },
    mainKeys,
    mfKeys,
    1, // sequence
    null, // previous
    Date.now() // timestamp
  )
  const msgVal1 = bendyButt.decode(bbmsg1)
  const msgKey1 = bendyButt.hash(msgVal1)

  const bbmsg2 = bendyButt.encodeNew(
    {
      type: 'metafeed/tombstone',
      subfeed: mainKeys.id,
      metafeed: mfKeys.id,
      tangles: {
        metafeed: {
          root: msgKey1,
          previous: msgKey1,
        },
      },
    },
    mainKeys,
    mfKeys,
    2, // sequence
    msgKey1, // previous
    Date.now() // timestamp
  )

  pull(
    pull.values([bbmsg1, bbmsg2]),
    pull.asyncMap((nativeMsg, cb) => db.add(nativeMsg, cb)),
    pull.collect(async (err) => {
      t.error(err)

      const results = await db.query(where(author(mfKeys.id)), toPromise())
      t.equals(results.length, 2)
      t.equal(results[0].value.content.type, 'metafeed/add/existing')
      t.equal(results[1].value.content.type, 'metafeed/tombstone')

      const results2 = await db.query(where(authorIsBendyButtV1()), toPromise())
      t.deepEquals(results2, results, 'authorIsBendyButtV1 works')

      t.end()
    })
  )
})

test('cannot add() gabbygrove-v1 messages (yet)', (t) => {
  const ggKeys = ssbKeys.generate(null, null, 'gabbygrove-v1')

  const msgVal = { author: ggKeys.id, the: 1, rest: 2, doesnt: 3, matter: 5 }

  db.add(msgVal, (err, x) => {
    t.match(
      err.message,
      /failed because feed format is unknown/,
      'expected error'
    )
    t.notOk(x, 'expected no result')
    t.end()
  })
})

test('validate needs to load', (t) => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.onDrain(() => {
    sbot.close(() => {
      sbot = SecretStack({ appKey: caps.shs })
        .use(require('../'))
        .use(require('../compat/ebt'))
        .call(null, {
          keys,
          path: dir,
        })
      db = sbot.db

      // make sure we can post from cold boot
      db.publish(post, (err, msg) => {
        t.error(err, 'no err')

        t.equal(msg.value.previous, null)

        db.onDrain(() => {
          sbot.close(() => {
            // reload
            sbot = SecretStack({ appKey: caps.shs })
              .use(require('../'))
              .use(require('../compat/ebt'))
              .call(null, {
                keys,
                path: dir,
              })
            db = sbot.db

            // make sure we have the correct previous
            db.publish(post2, (err, msg2) => {
              t.error(err, 'no err')
              t.equal(msg.key, msg2.value.previous)
              t.end()
            })
          })
        })
      })
    })
  })
})

test('validate when latest loaded was private message', (t) => {
  let secretPost = { type: 'post', text: 'Secret stuff', recps: [keys.id] }
  secretPost = ssbKeys.box(secretPost, [keys.id.substr(1)])

  db.publish(secretPost, (err, msg) => {
    t.error(err, 'no err')

    db.onDrain(() => {
      sbot.close(() => {
        // reload
        sbot = SecretStack({ appKey: caps.shs })
          .use(require('../'))
          .use(require('../compat/ebt'))
          .call(null, {
            keys,
            path: dir,
          })
        db = sbot.db

        let normalPost = { type: 'post', text: 'Public stuff' }
        db.publish(normalPost, (err, msg2) => {
          t.error(err, 'no err')
          t.equal(msg.key, msg2.value.previous)
          t.end()
        })
      })
    })
  })
})

test('publishAs classic', (t) => {
  const keys = ssbKeys.generate()

  const content = { type: 'post', text: 'hello world!' }

  db.publishAs(keys, content, (err, msg) => {
    t.error(err, 'no err')

    db.get(msg.key, (err, msg) => {
      t.equal(msg.content.type, 'post')
      t.equal(msg.sequence, 1)

      const content2 = { type: 'post', text: 'hello world 2!' }

      db.publishAs(keys, content2, (err, msg) => {
        t.error(err, 'no err')

        db.get(msg.key, (err, msg) => {
          t.equal(msg.content.text, 'hello world 2!')
          t.equal(msg.sequence, 2)

          t.end()
        })
      })
    })
  })
})

test('prepare()', (t) => {
  db.publish({ type: 'article', text: 'The News Today' }, (err, msg) => {
    t.error(err, 'no err')

    setTimeout(() => {
      t.ok(db.getJITDB().indexes)
      t.notOk(db.getJITDB().indexes['value_content_type_article'])
      db.prepare(type('article'), (err, duration) => {
        t.error(err, 'no err')
        t.ok(db.getJITDB().indexes)
        t.ok(db.getJITDB().indexes['value_content_type_article'])
        t.ok(duration)
        t.end()
      })
    }, 500)
  })
})

test('teardown', (t) => {
  sbot.close(t.end)
})
