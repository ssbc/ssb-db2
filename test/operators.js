const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const {and, type, toCallback, author} = require('../operators')

const dir = '/tmp/ssb-db2-operators'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const DB = require('../db')
const db = DB.init(dir, {
  path: dir,
  keys
})

test('execute and(type("post"), author(me))', t => {
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
