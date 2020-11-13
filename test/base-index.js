// add a bunch of message, add index see that it indexes correctly

// add messages after the index has been created, also test indexSync

const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')

const dir = '/tmp/ssb-db2-base-index'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const DB = require('../db')
const db = DB.init(dir, {
  path: dir,
  keys
})

test('drain', t => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.onDrain('base', () => {
      const status = db.getStatus()
      t.equal(status.log, 0, 'log in sync')
      t.equal(status.indexes['base'], 0, 'index in sync')
      t.end()
    })
  })
})

test('get', t => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(post2, (err, postMsg2) => {
      t.error(err, 'no err')

      db.onDrain('base', () => {
        db.get(postMsg.key, (err, msg) => {
          t.equal(msg.content.text, post.text, 'correct msg')

          t.end()
        })
      })
    })
  })
})

test('getsync', t => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(post2, (err, postMsg2) => {
      t.error(err, 'no err')

      db.getSync(postMsg.key, (err, msg) => {
        t.equal(msg.content.text, post.text, 'correct msg')

        t.end()
      })
    })
  })
})

test('author seq', t => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(post2, (err, postMsg2) => {
      t.error(err, 'no err')

      db.onDrain('base', () => {
        db.getMessageFromAuthorSequence([keys.id, postMsg2.value.sequence], (err, msg) => {
          t.error(err, 'no err')
          t.equal(msg.value.content.text, post2.text, 'correct msg')

          t.end()
        })
      })
    })
  })
})

test('get latest', t => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.onDrain('base', () => {
      db.getLatest(keys.id, (err, status) => {
        t.equal(postMsg.key, status.id)
        t.equal(postMsg.value.sequence, status.sequence)
        t.equal(postMsg.value.timestamp, status.timestamp)

        t.end()
      })
    })
  })
})

test('get all latest', t => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.onDrain('base', () => {
      db.getAllLatest((err, all) => {
        t.equal(Object.keys(all).length, 1, "authors")
        const status = all[keys.id]
        t.equal(postMsg.key, status.id)
        t.equal(postMsg.value.sequence, status.sequence)
        t.equal(postMsg.value.timestamp, status.timestamp)

        t.end()
      })
    })
  })
})
