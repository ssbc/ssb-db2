const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const pull = require('pull-stream')
const { and, live, toCallback, toPullStream } = require('../operators')
const mentions = require('../operators/full-mentions')

const dir = '/tmp/ssb-db2-mentions-index'

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

test('getMessagesByMention live', { timeout: 5000 }, (t) => {
  const feedId = '@abc'
  const mentionFeed = {
    type: 'post',
    text: 'Goodbye @abc',
    mentions: [{ link: feedId }],
  }
  const unrelatedMsg = {
    type: 'post',
    text: 'random',
  }

  pull(
    db.query(and(mentions(feedId)), live(), toPullStream()),
    pull.drain((msg) => {
      t.equal(msg.value.content.text, 'Goodbye @abc')
      t.end()
      return false // abort the drain
    })
  )

  setTimeout(() => {
    db.publish(unrelatedMsg, (err) => {
      t.pass('published unrelated new msg')
    })
  }, 200)
  setTimeout(() => {
    db.publish(mentionFeed, (err) => {
      t.pass('published related new msg')
    })
  }, 400)
})

test('teardown sbot', (t) => {
  sbot.close(t.end)
})
