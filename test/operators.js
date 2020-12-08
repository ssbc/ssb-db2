const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const pull = require('pull-stream')
const DB = require('../db')
const {
  and,
  type,
  isPrivate,
  toCallback,
  author,
  isRoot,
  votesFor,
  hasRoot,
  live,
  toPullStream,
} = require('../operators')

const dir = '/tmp/ssb-db2-operators'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const db = DB.init({}, dir, {
  path: dir,
  keys,
})

test('execute and(type("post"), author(me))', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.onDrain('base', () => {
      db.query(
        and(type('post'), author(keys.id)),
        toCallback((err2, msgs) => {
          t.error(err2, 'no err2')
          t.equal(msgs.length, 1)
          t.equal(msgs[0].value.content.type, 'post')
          t.end()
        })
      )
    })
  })
})

test('execute and(type("post"), isPrivate())', (t) => {
  let content = { type: 'post', text: 'super secret', recps: [keys.id] }
  content = ssbKeys.box(
    content,
    content.recps.map((x) => x.substr(1))
  )

  db.publish(content, (err, postMsg) => {
    t.error(err, 'no err')

    db.onDrain('base', () => {
      db.query(
        and(type('post'), isPrivate()),
        toCallback((err2, msgs) => {
          t.error(err2, 'no err2')
          t.equal(msgs.length, 1)
          t.equal(msgs[0].value.content.text, 'super secret')
          t.end()
        })
      )
    })
  })
})

test('execute isRoot()', (t) => {
  db.publish({ type: 'foo', text: 'Testing root!' }, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(
      { type: 'foo', text: 'Testing reply!', root: postMsg.key },
      (err2) => {
        t.error(err2, 'no err2')

        db.onDrain('base', () => {
          db.query(
            and(type('foo'), isRoot()),
            toCallback((err3, msgs) => {
              t.error(err3, 'no err3')
              t.equal(msgs.length, 1)
              t.equal(msgs[0].value.content.text, 'Testing root!')
              t.end()
            })
          )
        })
      }
    )
  })
})

test('execute hasRoot(msgkey)', (t) => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(post2, (err) => {
      t.error(err, 'no err')

      const threadMsg1 = { type: 'post', text: 'reply', root: postMsg.key }

      db.publish(threadMsg1, (err) => {
        t.error(err, 'no err')

        db.onDrain(() => {
          db.query(
            and(hasRoot(postMsg.key)),
            toCallback((err, results) => {
              t.error(err, 'no err')
              t.equal(results.length, 1)
              t.equal(results[0].value.content.text, threadMsg1.text)
              t.end()
            })
          )
        })
      })
    })
  })
})

test('hasRoot() outputs encrypted replies too', (t) => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(post2, (err) => {
      t.error(err, 'no err')

      let threadMsg1 = {
        type: 'post',
        text: 'encrypted reply',
        root: postMsg.key,
        recps: [keys.id],
      }
      threadMsg1 = ssbKeys.box(
        threadMsg1,
        threadMsg1.recps.map((x) => x.substr(1))
      )

      db.publish(threadMsg1, (err, privMsg) => {
        t.error(err, 'no err')

        db.onDrain(() => {
          db.query(
            and(hasRoot(postMsg.key)),
            toCallback((err, results) => {
              t.error(err, 'no err')
              t.equal(results.length, 1)
              t.equal(results[0].value.content.text, 'encrypted reply')
              t.end()
            })
          )
        })
      })
    })
  })
})

test('execute votesFor(msgkey)', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    const voteMsg1 = {
      type: 'vote',
      vote: {
        link: postMsg.key,
        value: 1,
        expression: '❤',
      },
    }

    const voteMsg2 = {
      type: 'vote',
      vote: {
        link: postMsg.key,
        value: -1,
        expression: '❤',
      },
    }

    db.publish(voteMsg1, (err, v1) => {
      t.error(err, 'no err')

      db.publish(voteMsg2, (err, v2) => {
        t.error(err, 'no err')

        db.onDrain(() => {
          db.query(
            and(votesFor(postMsg.key)),
            toCallback((err, results) => {
              t.error(err, 'no err')
              t.equal(results.length, 2)
              t.equal(results[0].key, v1.key)
              t.equal(results[1].key, v2.key)
              t.end()
            })
          )
        })
      })
    })
  })
})

test('live votesFor', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    const voteMsg1 = {
      type: 'vote',
      vote: {
        link: postMsg.key,
        value: 1,
        expression: '❤',
      },
    }

    const voteMsg2 = {
      type: 'vote',
      vote: {
        link: postMsg.key,
        value: -1,
        expression: '❤',
      },
    }

    db.publish(voteMsg1, (err, v1) => {
      t.error(err, 'no err')

      db.onDrain(() => {
        let i = 0
        pull(
          db.query(
            and(votesFor(postMsg.key)),
            live(),
            toPullStream(),
            pull.drain((result) => {
              if (i++ == 0) {
                t.equal(result.key, v1.key)

                db.publish(voteMsg2, (err, v2) => {
                  t.error(err, 'no err')
                })
              } else {
                t.equal(result.value.content.vote.value, -1)
                t.end()
              }
            })
          )
        )
      })
    })
  })
})
