const ssbKeys = require('ssb-keys')
const validate = require('ssb-validate')
const path = require('path')
const test = require('tape')
const pull = require('pull-stream')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')

const dir = '/tmp/ssb-db2-history-stream'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const sbot = SecretStack({ appKey: caps.shs })
  .use(require('../'))
  .use(require('../compat/db'))
  .use(require('../compat/history-stream'))
  .call(null, {
    keys,
    path: dir,
  })
const db = sbot.db

test('Base', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  let state = validate.initial()
  const otherKeys = ssbKeys.generate()
  const otherMsg = { type: 'post', text: 'test1' }

  state = validate.appendNew(state, null, otherKeys, otherMsg, Date.now())
  db.add(state.queue[0].value, (err) => {
    db.publish(post, (err, postMsg) => {
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

test('createWriteStream', (t) => {
  const rando = ssbKeys.generate()
  const post1 = { type: 'post', text: 'a' }
  const post2 = { type: 'post', text: 'b' }
  const post3 = { type: 'post', text: 'c' }

  let s = validate.initial()

  s = validate.appendNew(s, null, rando, post1, Date.now() + 1)
  s = validate.appendNew(s, null, rando, post2, Date.now() + 2)
  s = validate.appendNew(s, null, rando, post3, Date.now() + 3)

  let wrote = 0
  pull(
    pull.values(s.queue),
    pull.map((kvt) => kvt.value),
    pull.through(() => {
      wrote++
    }),
    sbot.createWriteStream((err) => {
      t.error(err)
      t.equals(wrote, 3)
      pull(
        sbot.createHistoryStream({ id: rando.id, values: true }),
        pull.collect((err2, results) => {
          t.equals(results.length, 3)
          t.equal(results[0].value.content.text, 'a')
          t.equal(results[1].value.content.text, 'b')
          t.equal(results[2].value.content.text, 'c')
          t.end()
        })
      )
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
      )
    })
  )
})

test('limit', (t) => {
  db.publish({ type: 'post', text: 'Testing 3' }, (err, postMsg) => {
    pull(
      sbot.createHistoryStream({ id: keys.id, limit: 1 }),
      pull.collect((err, results) => {
        t.equal(results.length, 1)
        t.equal(results[0].value.content.text, 'Testing!')

        pull(
          sbot.createHistoryStream({ id: keys.id }),
          pull.collect((err, results) => {
            t.equal(results.length, 3)
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
    pull(
      sbot.createHistoryStream({ id: keys.id, keys: false }),
      pull.collect((err, results) => {
        t.equal(results.length, 4)
        t.equal(typeof results[3].content, 'string')
        sbot.close(t.end)
      })
    )
  })
})

test('should be hookable', (t) => {
  let hookCalled = false
  sbot.createHistoryStream.hook(function (fn, args) {
    hookCalled = true
    return fn.call(null, args[0])
  })

  pull(
    sbot.createHistoryStream({ id: 'wat', limit: 1 }),
    pull.collect(() => {
      t.true(hookCalled)
      t.end()
    })
  )
})
