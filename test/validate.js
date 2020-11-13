const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')

const dir = '/tmp/ssb-db2-validate'

rimraf.sync(dir)
mkdirp.sync(dir)

const db = require('../db')
const ssbDB = db.init(dir, {
  path: dir,
  keys: ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
})

test('Base', t => {
  const post = { type: 'post', text: 'Testing!' }

  ssbDB.publish(post, (err, postMsg) => {
    t.error(err, 'no err')
    t.equal(postMsg.value.content.text, post.text, 'text correct')
    t.end()
  })
})

test('Multiple', t => {
  const post = { type: 'post', text: 'Testing!' }

  ssbDB.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    const post2 = { type: 'post', text: 'Testing 2!' }

    ssbDB.publish(post2, (err, postMsg2) => {
      t.error(err, 'no err')
      t.equal(postMsg2.value.content.text, post2.text, 'text correct')
      t.end()
    })
  })
})

test('Raw feed with unused type + ooo', t => {
  const validate = require('ssb-validate')
  var state = validate.initial()
  var keys = require('ssb-keys').generate()

  state = validate.appendNew(state, null, keys, { type: 'post', text: 'test1' }, Date.now()) // ooo
  state = validate.appendNew(state, null, keys, { type: 'post', text: 'test2' }, Date.now()+1) // missing
  state = validate.appendNew(state, null, keys, { type: 'post', text: 'test3' }, Date.now()+2) // start
  state = validate.appendNew(state, null, keys, { type: 'vote', vote: { link: '%something.sha256', value: 1 } }, Date.now()+3)
  state = validate.appendNew(state, null, keys, { type: 'post', text: 'test5' }, Date.now()+4)
  state = validate.appendNew(state, null, keys, { type: 'post', text: 'test6' }, Date.now()+5)

  ssbDB.validateAndAdd(state.queue[2].value, (err) => {
    t.error(err, 'no err')

    ssbDB.validateAndAdd(state.queue[3].value, (err) => {
      t.error(err, 'no err')

      ssbDB.validateAndAdd(state.queue[4].value, (err) => {
        t.error(err, 'no err')

        ssbDB.validateAndAdd(state.queue[5].value, (err) => {
          t.error(err, 'no err')

          ssbDB.validateAndAddOOO(state.queue[0].value, (err, oooMsg) => {
            t.error(err, 'no err')
            t.equal(oooMsg.value.content.text, 'test1', 'text correct')

            t.end()
          })
        })
      })
    })
  })
})

// we might get some messages from an earlier thread, and then get the latest 25 messages from the user
test('Add with holes', t => {
  const validate = require('ssb-validate')
  var state = validate.initial()
  var keys = require('ssb-keys').generate()

  state = validate.appendNew(state, null, keys, { type: 'post', text: 'test1' }, Date.now()) // ooo
  state = validate.appendNew(state, null, keys, { type: 'post', text: 'test2' }, Date.now()+1) // missing
  state = validate.appendNew(state, null, keys, { type: 'post', text: 'test3' }, Date.now()+2) // start

  ssbDB.validateAndAdd(state.queue[0].value, (err) => {
    t.error(err, 'no err')

    ssbDB.validateAndAddOOO(state.queue[2].value, (err, msg) => {
      t.error(err, 'no err')
      t.equal(msg.value.content.text, 'test3', 'text correct')
      t.end()
    })
  })
})

test('Add same message twice', t => {
  const validate = require('ssb-validate')
  var state = validate.initial()
  var keys = require('ssb-keys').generate()

  state = validate.appendNew(state, null, keys, { type: 'post', text: 'test1' }, Date.now())
  state = validate.appendNew(state, null, keys, { type: 'post', text: 'test2' }, Date.now()+1)

  ssbDB.validateAndAdd(state.queue[0].value, (err) => {
    t.error(err, 'no err')

    ssbDB.validateAndAdd(state.queue[1].value, (err) => {
      t.error(err, 'no err')

      ssbDB.validateAndAdd(state.queue[1].value, (err) => {
        t.ok(err, 'Should fail to add')
        t.end()
      })
    })
  })
})

test('Strict order basic case', t => {
  const validate = require('ssb-validate')
  var state = validate.initial()
  var keys = require('ssb-keys').generate()

  state = validate.appendNew(state, null, keys, { type: 'post', text: 'test1' }, Date.now())
  state = validate.appendNew(state, null, keys, { type: 'post', text: 'test2' }, Date.now()+1)

  ssbDB.validateAndAdd(state.queue[0].value, (err) => {
    t.error(err, 'no err')

    ssbDB.validateAndAdd(state.queue[1].value, (err) => {
      t.error(err, 'no err')
      t.end()
    })
  })
})

test('Strict order fail case', t => {
  const validate = require('ssb-validate')
  var state = validate.initial()
  var keys = require('ssb-keys').generate()

  state = validate.appendNew(state, null, keys, { type: 'post', text: 'test1' }, Date.now())
  state = validate.appendNew(state, null, keys, { type: 'post', text: 'test2' }, Date.now()+1)
  state = validate.appendNew(state, null, keys, { type: 'post', text: 'test3' }, Date.now()+2)

  ssbDB.validateAndAdd(state.queue[0].value, (err) => {
    t.error(err, 'no err')

    ssbDB.validateAndAdd(state.queue[2].value, (err) => {
      t.ok(err, 'Should fail to add')

      t.end()
    })
  })
})
