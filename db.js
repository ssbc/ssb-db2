const push = require('push-stream')
const hash = require('ssb-keys/util').hash
const validate = require('ssb-validate')
const keys = require('ssb-keys')
const path = require('path')
const Obv = require('obv')
const jitdbOperators = require('jitdb/operators')
const JITDb = require('jitdb')

const Log = require('./log')
const BaseIndex = require('./indexes/base')
const Partial = require('./indexes/partial')

function getId(msg) {
  return '%'+hash(JSON.stringify(msg, null, 2))
}

exports.init = function (dir, config) {
  const log = Log(dir, config)
  const jitdb = JITDb(log, path.join(dir, 'db2', 'indexes'))
  const baseIndex = BaseIndex(log, dir, config.keys.public)
  //const contacts = fullIndex.contacts
  //const partial = Partial(dir)

  const indexes = {
    base: baseIndex
  }
  const post = Obv()
  const hmac_key = null
  let state = validate.initial()

  // restore current state
  baseIndex.getAllLatest((err, last) => {
    // copy to so we avoid weirdness, because this object
    // tracks the state coming in to the database.
    for (const k in last) {
      state.feeds[k] = {
        id: last[k].id,
        timestamp: last[k].timestamp,
        sequence: last[k].sequence,
        queue: []
      }
    }
  })

  function get(id, cb) {
    baseIndex.getMessageFromKey(id, (err, data) => {
      if (data)
        cb(null, data.value)
      else
        cb(err)
    })
  }

  function add(msg, cb) {
    const id = getId(msg)

    /*
      Beware:

      There is a race condition here if you add the same message quickly
      after another because baseIndex is lazy. The default js SSB
      implementation adds messages in order, so it doesn't really have
      this problem.
    */

    baseIndex.getMessageFromKey(id, (err, data) => {
      if (data)
        cb(null, data.value)
      else {
        // store encrypted messages for us unencrypted for views
        // ebt will turn messages into encrypted ones before sending
        if (typeof (msg.content) === 'string') {
          const decrypted = keys.unbox(msg.content, config.keys.private)
          if (decrypted) {
            const cyphertext = msg.content

            msg.content = decrypted
            msg.meta = {
	      private: "true",
	      original: {
	        content: cyphertext
	      }
            }
          }
        }

        log.add(id, msg, cb)
      }
    })
  }

  function publish(msg, cb) {
    state.queue = []
    state = validate.appendNew(state, null, config.keys, msg, Date.now())
    add(state.queue[0].value, (err, data) => {
      post.set(data)
      cb(err, data)
    })
  }

  function del(key, cb) {
    baseIndex.keyToSeq(key, (err, seq) => {
      if (err) return cb(err)
      if (seq == null) return cb(new Error('seq is null!'))

      log.del(seq, cb)
    })
  }

  function deleteFeed(feedId, cb) {
    // FIXME: doesn't work, need test
    jitdb.onReady(() => {
      jitdb.query({
        type: 'EQUAL',
        data: {
          seek: jitdb.seekAuthor,
          value: feedId,
          indexType: "author"
        }
      }, (err, results) => {
        push(
          push.values(results),
          push.asyncMap((msg, cb) => {
            del(msg.key, cb)
          }),
          push.collect((err) => {
            if (!err) {
              delete state.feeds[feedId]
              baseIndex.removeFeedFromLatest(feedId)
            }
            cb(err)
          })
        )
      })
    })
  }

  function decryptMessage(msg) {
    return keys.unbox(msg.content, config.keys.private)
  }

  function validateAndAddOOO(msg, cb) {
    try {
      let oooState = validate.initial()
      validate.appendOOO(oooState, hmac_key, msg)

      if (oooState.error)
        return cb(oooState.error)

      add(msg, cb)
    }
    catch (ex)
    {
      return cb(ex)
    }
  }

  function validateAndAdd(msg, cb) {
    const knownAuthor = msg.author in state.feeds

    try {
      if (!knownAuthor)
        state = validate.appendOOO(state, hmac_key, msg)
      else
        state = validate.append(state, hmac_key, msg)

      if (state.error)
        return cb(state.error)

      add(msg, cb)
    }
    catch (ex)
    {
      return cb(ex)
    }
  }

  function getStatus() {
    //const partialState = partial.getSync()
    //const graph = contacts.getGraphForFeedSync(config.keys.public)

    // partial
    /*
    let profilesSynced = 0
    let contactsSynced = 0
    let messagesSynced = 0
    let totalPartial = 0
    */

    // full
    let fullSynced = 0
    let totalFull = 0

    /*
    graph.following.forEach(relation => {
      if (partialState[relation] && partialState[relation]['full'])
        fullSynced += 1

      totalFull += 1
    })

    graph.extended.forEach(relation => {
      if (partialState[relation] && partialState[relation]['syncedProfile'])
        profilesSynced += 1
      if (partialState[relation] && partialState[relation]['syncedContacts'])
        contactsSynced += 1
      if (partialState[relation] && partialState[relation]['syncedMessages'])
        messagesSynced += 1

      totalPartial += 1
    })
    */

    const result = {
      log: log.since.value,
      indexes: {},
      /*
      partial: {
        totalPartial,
        profilesSynced,
        contactsSynced,
        messagesSynced,
        totalFull,
        fullSynced,
      }
      */
    }

    for (const indexName in indexes) {
      result.indexes[indexName] = indexes[indexName].seq.value
    }

    return result
  }

  function clearIndexes() {
    for (const indexName in indexes)
      indexes[indexName].remove(() => {})
  }

  function registerIndex(Index) {
    const index = Index(log, jitdb, dir, config.keys.public)

    if (indexes[index.name]) throw "Index already exists"

    indexes[index.name] = index
  }

  function onDrain(indexName, cb) {
    if (!cb) { // default
      cb = indexName
      indexName = 'base'
    }

    log.onDrain(() => {
      const index = indexes[indexName]
      if (!index) return cb('Unknown index:' + indexName)

      if (index.seq.value === log.since.value) {
        cb()
      } else {
        const remove = index.seq(() => {
          if (index.seq.value === log.since.value) {
            remove()
            cb()
          }
        })
      }
    })
  }

  // override query() from jitdb to implicitly call fromDB()
  function query(first, ...rest) {
    if (!first.meta) {
      const ops = jitdbOperators.fromDB(jitdb)
      ops.meta.db2 = this
      return jitdbOperators.query(ops, first, ...rest)
    } else {
      return jitdbOperators.query(first, ...rest)
    }
  }

  return {
    get,
    getSync: function(id, cb) {
      onDrain('base', () => {
        get(id, cb)
      })
    },
    add,
    publish,
    del,
    deleteFeed,
    validateAndAdd,
    validateAndAddOOO,
    getStatus,

    post,

    registerIndex,
    indexes,

    getLatest: baseIndex.getLatest,
    getAllLatest: baseIndex.getAllLatest,
    getMessageFromAuthorSequence: baseIndex.getMessageFromAuthorSequence,

    // FIXME: contacts & profiles

    jitdb,
    onDrain,
    query,

    // hack
    state,

    // debugging
    clearIndexes,

    // partial stuff
    //partial
  }
}
