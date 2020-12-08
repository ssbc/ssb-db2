const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const DB = require('../db')
const Mentions = require('../indexes/mentions')
const { and, toCallback } = require('../operators')
const mentions = require('../operators/mentions')

const dir = '/tmp/ssb-db2-mentions-index'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const db = DB.init({}, dir, {
  path: dir,
  keys,
})

db.registerIndex(Mentions)

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

        db.onDrain('mentions', () => {
          const status = db.getStatus()
          t.equal(status.indexes['mentions'], 780, 'index in sync')

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
