const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const pull = require('pull-stream')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const {
  and,
  or,
  type,
  isPrivate,
  isPublic,
  toCallback,
  author,
  isRoot,
  votesFor,
  mentions,
  hasRoot,
  hasFork,
  hasBranch,
  live,
  toPullStream,
  contact,
} = require('../operators')

const dir = '/tmp/ssb-db2-operators'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const sbot = SecretStack({ appKey: caps.shs })
  .use(require('../'))
  .use(require('../full-mentions'))
  .call(null, {
    keys,
    path: dir,
  })
const db = sbot.db

test('can create a reusable query portion', (t) => {
  const about = { type: 'about', text: 'Testing!' }

  db.publish(about, (err, postMsg) => {
    t.error(err, 'no err')

    const myAbouts = db.query(and(type('about'), author(keys.id)))

    db.query(
      myAbouts,
      toCallback((err2, msgs) => {
        t.error(err2, 'no err2')
        t.equal(msgs.length, 1)
        t.equal(msgs[0].value.content.type, 'about')

        pull(
          db.query(myAbouts, toPullStream()),
          pull.collect((err3, msgsAgain) => {
            t.error(err3, 'no err3')
            t.equal(msgsAgain.length, 1)
            t.equal(msgsAgain[0].value.content.type, 'about')
            t.end()
          })
        )
      })
    )
  })
})

test('execute and(type("post"), author(me))', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

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

test('execute and(type("post"), isPrivate())', (t) => {
  let content = { type: 'post', text: 'super secret', recps: [keys.id] }
  content = ssbKeys.box(
    content,
    content.recps.map((x) => x.substr(1))
  )

  db.publish(content, (err, postMsg) => {
    t.error(err, 'no err')

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

test('execute and(type("post"), isPublic())', (t) => {
  let content = { type: 'posty', text: 'super secret', recps: [keys.id] }
  content = ssbKeys.box(
    content,
    content.recps.map((x) => x.substr(1))
  )

  db.publish(content, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish({ type: 'posty', text: 'Testing public' }, (err, postMsg) => {
      db.query(
        and(type('posty'), isPublic()),
        toCallback((err2, msgs) => {
          t.error(err2, 'no err2')
          t.equal(msgs.length, 1)
          t.equal(msgs[0].value.content.text, 'Testing public')
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

        db.query(
          and(type('foo'), isRoot()),
          toCallback((err3, msgs) => {
            t.error(err3, 'no err3')
            t.equal(msgs.length, 1)
            t.equal(msgs[0].value.content.text, 'Testing root!')
            t.end()
          })
        )
      }
    )
  })
})

test('execute and(hasRoot(msgkey))', (t) => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(post2, (err) => {
      t.error(err, 'no err')

      const threadMsg1 = { type: 'post', text: 'reply', root: postMsg.key }

      db.publish(threadMsg1, (err) => {
        t.error(err, 'no err')

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

test('execute or(hasRoot(msgkey))', (t) => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(post2, (err) => {
      t.error(err, 'no err')

      const threadMsg1 = { type: 'post', text: 'reply', root: postMsg.key }

      db.publish(threadMsg1, (err) => {
        t.error(err, 'no err')

        db.query(
          or(hasRoot(postMsg.key)),
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

test('execute hasFork(msgkey)', (t) => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(post2, (err) => {
      t.error(err, 'no err')

      const threadMsg1 = { type: 'post', text: 'reply', fork: postMsg.key }

      db.publish(threadMsg1, (err) => {
        t.error(err, 'no err')

        db.query(
          and(hasFork(postMsg.key)),
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

test('execute hasBranch(msgkey)', (t) => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(post2, (err) => {
      t.error(err, 'no err')

      const threadMsg1 = { type: 'post', text: 'reply', branch: postMsg.key }

      db.publish(threadMsg1, (err) => {
        t.error(err, 'no err')

        db.query(
          and(hasBranch(postMsg.key)),
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

test('execute mentions(feedid) and mentions(msgkey)', (t) => {
  const post = { type: 'post', text: 'Testing!' }
  const feedId = '@abc'
  const mentionFeed = {
    type: 'post',
    text: 'Hello @abc',
    mentions: [{ link: feedId }],
  }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(mentionFeed, (err) => {
      t.error(err, 'no err')

      const mentionMsg = {
        type: 'post',
        text: `What is [this](${postMsg.key})`,
        mentions: [{ link: postMsg.key }],
      }

      db.publish(mentionMsg, (err) => {
        t.error(err, 'no err')

        db.query(
          and(mentions(feedId)),
          toCallback((err, results) => {
            t.error(err, 'no err')
            t.equal(results.length, 1)
            t.equal(results[0].value.content.text, mentionFeed.text)

            db.query(
              and(mentions(postMsg.key)),
              toCallback((err2, results2) => {
                t.error(err2, 'no err')
                t.equal(results2.length, 1)
                t.equal(results2[0].value.content.text, mentionMsg.text)
                t.end()
              })
            )
          })
        )
      })
    })
  })
})

test('execute contact(feedid)', (t) => {
  const feedid = ssbKeys.generate().id
  const msg1 = { type: 'contact', contact: feedid, following: true }
  const msg2 = { type: 'post', text: 'Testing!' }

  db.publish(msg1, (err, m1) => {
    t.error(err, 'no err')
    db.publish(msg2, (err, m2) => {
      t.error(err, 'no err')

      db.query(
        and(contact(feedid)),
        toCallback((err, results) => {
          t.error(err, 'no err')
          t.equal(results.length, 1)
          t.equal(results[0].value.content.following, true)
          t.end()
        })
      )
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

test('operators are exposed', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    pull(
      db.query(
        db.operators.and(db.operators.key(postMsg.key)),
        db.operators.toCallback((err, results) => {
          t.equal(results[0].key, postMsg.key)
          t.end()
        })
      )
    )
  })
})

test('extra operators are exposed', (t) => {
  t.equal(typeof db.operators.fullMentions, 'function')
  t.end()
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

      let i = 0
      pull(
        db.query(
          and(votesFor(postMsg.key)),
          live({ old: true }),
          toPullStream()
        ),
        pull.drain(
          (result) => {
            if (i++ == 0) {
              t.equal(result.key, v1.key)
              db.publish(voteMsg2, (err, v2) => {
                t.error(err, 'no err')
              })
            } else {
              t.equal(result.value.content.vote.value, -1)
              t.end()
            }
          },
          () => {}
        )
      )
    })
  })
})

test('live alone', (t) => {
  pull(
    db.query(live({ old: true }), toPullStream()),
    pull.take(1),
    pull.drain((msg) => {
      t.ok(msg)
      t.ok(msg.value)
      t.ok(msg.value.content)
      t.end()
    })
  )
})

test('teardown sbot', (t) => {
  sbot.close(t.end)
})
