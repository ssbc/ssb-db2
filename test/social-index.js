const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')

const {query, toCallback} = require('jitdb/operators')

const rimraf = require('rimraf')
const mkdirp = require('mkdirp')

const dir = '/tmp/ssb-db2-social-index'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const DB = require('../db')
const db = DB.init(dir, {
  path: dir,
  keys
})

db.registerIndex(require('../indexes/social'))

test('getMessagesByMention', t => {
  const post = { type: 'post', text: 'Testing!' }
  const feedId = '@abc'
  const mentionFeed = { type: 'post', text: 'Hello @abc',
                        mentions: [ { link: feedId } ] }
  
  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(mentionFeed, (err) => {
      t.error(err, 'no err')

      const mentionMsg = { type: 'post',
                           text: `What is [this](${postMsg.key})`,
                           mentions: [{ link: postMsg.key }] }

      db.publish(mentionMsg, (err) => {
        t.error(err, 'no err')

        db.onDrain('social', () => {
          const status = db.getStatus()
          t.equal(status.indexes['social'], 780, 'index in sync')

          const social = db.indexes['social']
          social.getMessagesByMention(feedId, (err, q) => {
            t.error(err, 'no err')

            query(
              q,
              toCallback((err, results) => {
                t.equal(results.length, 1)
                t.equal(results[0].value.content.text, mentionFeed.text)

                social.getMessagesByMention(postMsg.key, (err, q) => {
                  t.error(err, 'no err')

                  query(
                    q,
                    toCallback((err, results) => {
                      t.equal(results.length, 1)
                      t.equal(results[0].value.content.text, mentionMsg.text)
                      t.end()
                    })
                  )
                })
              })
            )
          })
        })
      })
    })
  })
})

test('getMessagesByRoot', t => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(post2, (err) => {
      t.error(err, 'no err')

      const threadMsg1 = { type: 'post',
                           text: 'reply',
                           root: postMsg.key }

      db.publish(threadMsg1, (err) => {
        t.error(err, 'no err')

        db.onDrain('social', () => {
          const social = db.indexes['social']
          social.getMessagesByRoot(postMsg.key, (err, q) => {
            // doesn't include the root itself
            t.error(err, 'no err')

            query(
              q,
              toCallback((err, results) => {
                console.log(results)
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
})

test('getMessagesByVoteLink', t => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    const voteMsg1 = {
      type: 'vote',
      vote: {
        link: postMsg.key,
        value: 1,
        expression: '❤'
      }
    }

    const voteMsg2 = {
      type: 'vote',
      vote: {
        link: postMsg.key,
        value: -1,
        expression: '❤'
      }
    }

    db.publish(voteMsg1, (err, v1) => {
      t.error(err, 'no err')

      db.publish(voteMsg2, (err, v2) => {
        t.error(err, 'no err')

        db.onDrain('social', () => {
          const social = db.indexes['social']
          social.getMessagesByVoteLink(postMsg.key, (err, q) => {
            t.error(err, 'no err')

            query(
              q,
              toCallback((err, results) => {
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
})
