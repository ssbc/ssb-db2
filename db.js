const push = require('push-stream')

const hash = require('ssb-keys/util').hash
const validate = require('ssb-validate')
const keys = require('ssb-keys')
const path = require('path')

const Log = require('./log')
const FullScanIndexes = require('./indexes/full-scan')
const Partial = require('./indexes/partial')
const JITDb = require('jitdb')

function getId(msg) {
  return '%'+hash(JSON.stringify(msg, null, 2))
}

exports.init = function (dir, config) {
  const log = Log(dir, config)
  const jitdb = JITDb(log, path.join(dir, "indexes"))
  const fullIndex = FullScanIndexes(log, dir, config.keys.public)
  const contacts = fullIndex.contacts
  const partial = Partial(dir)

  function get(id, cb) {
    fullIndex.getDataFromKey(id, (err, data) => {
      if (data)
        cb(null, data.value)
      else
        cb(err)
    })
  }

  function getSync(id, cb) {
    if (fullIndex.seq.value === log.since.value) {
      get(id, cb)
    } else {
      var remove = fullIndex.seq(() => {
        if (fullIndex.seq.value === log.since.value) {
          remove()
          get(id, cb)
        }
      })
    }
  }

  function add(msg, cb) {
    var id = getId(msg)

    /*
      Beware:

      There is a race condition here if you add the same message quickly
      after another because fullIndex is lazy. The default js SSB
      implementation adds messages in order, so it doesn't really have
      this problem.
    */

    fullIndex.getDataFromKey(id, (err, data) => {
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

  var state = validate.initial()
  
  function publish(msg, cb) {
    state.queue = []
    state = validate.appendNew(state, null, config.keys, msg, Date.now())
    add(state.queue[0].value, (err, data) => {
      // FIXME: maybe re-add post obv again for EBT
      cb(err, data)
    })
  }
  
  function del(key, cb) {
    fullIndex.keyToSeq(key, (err, seq) => {
      if (err) return cb(err)
      if (seq == null) return cb(new Error('seq is null!'))

      log.del(seq, cb)
    })
  }

  function deleteFeed(feedId, cb) {
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
              fullIndex.removeFeedFromLatest(feedId)
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

  const hmac_key = null

  function validateAndAddOOO(msg, cb) {
    try {
      var state = validate.initial()
      validate.appendOOO(state, hmac_key, msg)

      if (state.error)
        return cb(state.error)

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
    const partialState = partial.getSync()
    const graph = contacts.getGraphForFeedSync(config.keys.public)

    // partial
    let profilesSynced = 0
    let contactsSynced = 0
    let messagesSynced = 0
    let totalPartial = 0

    // full
    let fullSynced = 0
    let totalFull = 0

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

    return {
      log: log.since.value,
      indexes: fullIndex.seq.value,
      partial: {
        totalPartial,
        profilesSynced,
        contactsSynced,
        messagesSynced,
        totalFull,
        fullSynced,
      }
    }
  }

  function clearIndexes() {
    fullIndex.remove(() => {})
  }

  return {
    get,
    getSync,
    add,
    publish,
    del,
    deleteFeed,
    validateAndAdd,
    validateAndAddOOO,
    getStatus,
    getAllLatest: fullIndex.getAllLatest,
    getDataFromAuthorSequence: fullIndex.getDataFromAuthorSequence,
    contacts,
    profiles: fullIndex.profiles,
    getMessagesByRoot: fullIndex.getMessagesByRoot,
    getMessagesByMention: fullIndex.getMessagesByMention,
    getMessagesByVoteLink: fullIndex.getMessagesByVoteLink,
    jitdb,
    onDrain: log.onDrain,
    getLatest: fullIndex.getLatest,

    // debugging
    clearIndexes,

    // partial stuff
    partial
  }
}
