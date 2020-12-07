const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const pull = require('pull-stream')
const DB = require('../db')
const Social = require('../indexes/social')
const { and, toCallback, live, toPullStream } = require('../operators')
const { hasRoot, votesFor, mentions } = require('../operators/social')

const dir = '/tmp/ssb-db2-social-index'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const db = DB.init({}, dir, {
  path: dir,
  keys,
})

db.registerIndex(Social)

test('getMessagesByMention', (t) => {
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

        db.onDrain('social', () => {
          const status = db.getStatus()
          t.equal(status.indexes['social'], 780, 'index in sync')

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
})

test('getMessagesByRoot', (t) => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(post2, (err) => {
      t.error(err, 'no err')

      const threadMsg1 = { type: 'post', text: 'reply', root: postMsg.key }

      db.publish(threadMsg1, (err) => {
        t.error(err, 'no err')

        db.onDrain('social', () => {
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

test('encrypted getMessagesByRoot', (t) => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(post2, (err) => {
      t.error(err, 'no err')

      let threadMsg1 = {
        type: 'post',
        text: 'reply',
        root: postMsg.key,
        recps: [keys.id],
      }
      threadMsg1 = ssbKeys.box(
        threadMsg1,
        threadMsg1.recps.map((x) => x.substr(1))
      )

      db.publish(threadMsg1, (err, privMsg) => {
        t.error(err, 'no err')

        db.onDrain('social', () => {
          db.query(
            and(hasRoot(postMsg.key)),
            toCallback((err, results) => {
              t.error(err, 'no err')
              t.equal(results.length, 1)
              t.equal(results[0].value.content.text, 'reply')
              t.end()
            })
          )
        })
      })
    })
  })
})

test('votesFor using prefix indexes', (t) => {
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

        db.onDrain('social', () => {
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

test('lives votesFor', (t) => {
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

      db.onDrain('social', () => {
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
