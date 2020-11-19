const ssbKeys = require('ssb-keys')
const validate = require('ssb-validate')
const path = require('path')
const test = require('tape')
const pull = require('pull-stream')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const DB = require('../db')
const HistoryCompat = require('../compat/history-stream')

const dir = '/tmp/ssb-db2-history-stream'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
const db = DB.init({}, dir, {
  path: dir,
  keys,
})

// simulate secret stack
const sbot = { db }
HistoryCompat.init(sbot)

test('Base', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  let state = validate.initial()
  const otherKeys = ssbKeys.generate()
  const otherMsg = { type: 'post', text: 'test1' }

  state = validate.appendNew(state, null, otherKeys, otherMsg, Date.now())
  db.add(state.queue[0].value, (err) => {
    db.publish(post, (err, postMsg) => {
      db.onDrain('base', () => {
        pull(
          sbot.createHistoryStream({ id: keys.id, keys: false }),
          pull.collect((err, results) => {
            t.equal(results.length, 1)
            // values directly
            t.equal(results[0].content.text, post.text)
            t.end()
          })
        )
      })
    })
  })
})

test('Keys', (t) => {
  pull(
    sbot.createHistoryStream({ id: keys.id }),
    pull.collect((err, results) => {
      t.equal(results.length, 1)
      t.equal(typeof results[0].key, 'string')
      t.end()
    })
  )
})

test('No values', (t) => {
  pull(
    sbot.createHistoryStream({ id: keys.id, values: false }),
    pull.collect((err, results) => {
      t.equal(results.length, 1)
      t.equal(typeof results[0], 'string')
      t.end()
    })
  )
})

test('Seq', (t) => {
  pull(
    sbot.createHistoryStream({ id: keys.id, keys: false, seq: 1 }),
    pull.collect((err, results) => {
      t.equal(results.length, 1)

      pull(
        sbot.createHistoryStream({ id: keys.id, keys: false, seq: 0 }),
        pull.collect((err, results) => {
          t.equal(results.length, 1)

          const post = { type: 'post', text: 'Testing 2' }
          db.publish(post, (err, postMsg) => {
            db.onDrain(() => {
              pull(
                sbot.createHistoryStream({ id: keys.id, keys: false, seq: 2 }),
                pull.collect((err, results) => {
                  t.equal(results.length, 1)
                  t.equal(results[0].content.text, post.text)

                  pull(
                    sbot.createHistoryStream({
                      id: keys.id,
                      keys: false,
                      seq: 1,
                      limit: 1,
                    }),
                    pull.collect((err, results) => {
                      t.equal(results.length, 1)
                      t.equal(results[0].content.text, 'Testing!')

                      t.end()
                    })
                  )
                })
              )
            })
          })
        })
      )
    })
  )
})

test('limit', (t) => {
  db.publish({ type: 'post', text: 'Testing 2' }, (err, postMsg) => {
    pull(
      sbot.createHistoryStream({ id: keys.id, limit: 1 }),
      pull.collect((err, results) => {
        t.equal(results.length, 1)
        t.equal(results[0].value.content.text, 'Testing!')

        pull(
          sbot.createHistoryStream({ id: keys.id }),
          pull.collect((err, results) => {
            t.equal(results.length, 2)
            t.end()
          })
        )
      })
    )
  })
})

test('non feed should err', (t) => {
  pull(
    sbot.createHistoryStream({ id: 'wat', limit: 1 }),
    pull.collect((err, results) => {
      t.equal(err, 'wat is not a feed')
      t.end()
    })
  )
})

test('Encrypted', (t) => {
  let content = { type: 'post', text: 'super secret', recps: [keys.id] }
  content = ssbKeys.box(
    content,
    content.recps.map((x) => x.substr(1))
  )

  db.publish(content, (err, privateMsg) => {
    db.onDrain(() => {
      pull(
        sbot.createHistoryStream({ id: keys.id, keys: false }),
        pull.collect((err, results) => {
          t.equal(results.length, 4)
          t.equal(typeof results[3].content, 'string')
          t.end()
        })
      )
    })
  })
})
