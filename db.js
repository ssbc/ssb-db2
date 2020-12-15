const push = require('push-stream')
const hash = require('ssb-keys/util').hash
const validate = require('ssb-validate')
const Obv = require('obv')
const promisify = require('promisify-4loc')
const jitdbOperators = require('jitdb/operators')
const JITDb = require('jitdb')
const Debug = require('debug')
const DeferredPromise = require('p-defer')

const { indexesPath } = require('./defaults')
const Log = require('./log')
const BaseIndex = require('./indexes/base')
const Private = require('./indexes/private')
// const Partial = require('./indexes/partial')

const {
  and,
  fromDB,
  key,
  author,
  deferred,
  toCallback,
} = require('./operators')

function getId(msg) {
  return '%' + hash(JSON.stringify(msg, null, 2))
}

exports.name = 'db'

exports.version = '0.7.0'

exports.manifest = {
  get: 'async',
  add: 'async',
  publish: 'async',
  del: 'async',
  deleteFeed: 'async',
  validateAndAdd: 'async',
  validateAndAddOOO: 'async',
  getStatus: 'sync',

  // `query` should be `sync`, but secret-stack is automagically converting it
  // to async because of secret-stack/utils.js#hookOptionalCB. Eventually we
  // should include an option `synconly` in secret-stack that bypasses the hook,
  // but for now we leave the `query` API *implicitly* available in the plugin:

  // query: 'sync',
}

exports.init = function (sbot, config) {
  const dir = config.path
  const private = Private(dir, config.keys)
  const log = Log(dir, config, private)
  const jitdb = JITDb(log, indexesPath(dir))
  const baseIndex = BaseIndex(log, dir, private)
  //const contacts = fullIndex.contacts
  //const partial = Partial(dir)

  const debug = Debug('ssb:db2')

  const indexes = {
    base: baseIndex,
  }
  const post = Obv()
  const hmac_key = null
  const stateFeedsReady = DeferredPromise()
  let state = validate.initial()

  sbot.close.hook(function (fn, args) {
    close(() => {
      fn.apply(this, args)
    })
  })

  // restore current state
  baseIndex.getAllLatest((err, last) => {
    // copy to so we avoid weirdness, because this object
    // tracks the state coming in to the database.
    for (const k in last) {
      state.feeds[k] = {
        id: last[k].id,
        timestamp: last[k].timestamp,
        sequence: last[k].sequence,
        queue: [],
      }
    }
    debug('getAllLatest is done setting up initial validate state')
    stateFeedsReady.resolve()
  })

  function guardAgainstDuplicateLogs(methodName) {
    if (sbot.db2migrate && sbot.db2migrate.doesOldLogExist()) {
      return new Error(
        'ssb-db2: refusing to ' +
          methodName +
          ' because the old log still exists. ' +
          'This is to protect your feed from forking ' +
          'into an irrecoverable state.'
      )
    }
  }

  function get(id, cb) {
    query(
      and(key(id)),
      toCallback((err, results) => {
        if (err) return cb(err)
        else if (results.length) return cb(null, results[0].value)
        else return cb()
      })
    )
  }

  function add(msg, cb) {
    const guard = guardAgainstDuplicateLogs('add()')
    if (guard) return cb(guard)

    const id = getId(msg)

    /*
      Beware:

      There is a race condition here if you add the same message quickly
      after another because baseIndex is lazy. The default js SSB
      implementation adds messages in order, so it doesn't really have
      this problem.
    */

    stateFeedsReady.promise.then(() => {
      get(id, (err, data) => {
        if (data) cb(null, data)
        else log.add(id, msg, cb)
      })
    })
  }

  function publish(msg, cb) {
    const guard = guardAgainstDuplicateLogs('publish()')
    if (guard) return cb(guard)

    state.queue = []
    state = validate.appendNew(state, null, config.keys, msg, Date.now())
    add(state.queue[0].value, (err, data) => {
      post.set(data)
      cb(err, data)
    })
  }

  function del(msgId, cb) {
    const guard = guardAgainstDuplicateLogs('del()')
    if (guard) return cb(guard)

    jitdb.all(key(msgId), 0, false, true, (err, results) => {
      if (err) return cb(err)
      if (results.length === 0) return cb(new Error('seq is null!'))

      log.del(results[0], cb)
    })
  }

  function deleteFeed(feedId, cb) {
    const guard = guardAgainstDuplicateLogs('deleteFeed()')
    if (guard) return cb(guard)

    jitdb.all(author(feedId), 0, false, true, (err, results) => {
      push(
        push.values(results),
        push.asyncMap((seq, cb) => {
          log.del(seq, cb)
        }),
        push.collect((err) => {
          if (err) cb(err)
          else {
            delete state.feeds[feedId]
            baseIndex.removeFeedFromLatest(feedId)
          }
        })
      )
    })
  }

  function validateAndAddOOO(msg, cb) {
    const guard = guardAgainstDuplicateLogs('validateAndAddOOO()')
    if (guard) return cb(guard)

    try {
      let oooState = validate.initial()
      validate.appendOOO(oooState, hmac_key, msg)

      if (oooState.error) return cb(oooState.error)

      add(msg, cb)
    } catch (ex) {
      return cb(ex)
    }
  }

  function validateAndAdd(msg, cb) {
    const guard = guardAgainstDuplicateLogs('validateAndAdd()')
    if (guard) return cb(guard)

    const knownAuthor = msg.author in state.feeds

    try {
      if (!knownAuthor) state = validate.appendOOO(state, hmac_key, msg)
      else state = validate.append(state, hmac_key, msg)

      if (state.error) return cb(state.error)

      add(msg, cb)
    } catch (ex) {
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
    for (const indexName in indexes) indexes[indexName].remove(() => {})
  }

  function registerIndex(Index) {
    const index = Index(log, dir)

    if (indexes[index.name]) throw 'Index already exists'

    indexes[index.name] = index
  }

  function updateIndexes() {
    const start = Date.now()

    const indexesRun = Object.values(indexes)

    function liveStream() {
      debug('live streaming changes')
      log.stream({ gt: indexes['base'].seq.value, live: true }).pipe({
        paused: false,
        write: (data) => indexesRun.forEach((x) => x.onData(data, true)),
      })
    }

    const lowestSeq = Math.min(
      ...Object.values(indexes).map((x) => x.seq.value)
    )
    debug(`lowest seq for all indexes ${lowestSeq}`)

    log.stream({ gt: lowestSeq }).pipe({
      paused: false,
      write: (data) => indexesRun.forEach((x) => x.onData(data, false)),
      end: () => {
        const tasks = indexesRun.map((index) => promisify(index.writeBatch)())
        Promise.all(tasks).then(liveStream)

        debug(`index scan time: ${Date.now() - start}ms`)
      },
    })
  }

  function onDrain(indexName, cb) {
    if (!cb) {
      // default
      cb = indexName
      indexName = 'base'
    }

    onIndexesStateLoaded(() => {
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
    })
  }

  function onIndexesStateLoaded(cb) {
    if (!onIndexesStateLoaded.promise) {
      const stateLoadedPromises = [private.stateLoaded]
      for (var index in indexes) {
        stateLoadedPromises.push(indexes[index].stateLoaded)
      }
      onIndexesStateLoaded.promise = Promise.all(stateLoadedPromises)
    }
    onIndexesStateLoaded.promise.then(cb)
  }

  // setTimeout to make sure extra indexes from secret-stack are also included
  setTimeout(() => {
    onIndexesStateLoaded(updateIndexes)
  }).unref()

  function close(cb) {
    const tasks = []
    tasks.push(promisify(log.close)())
    for (const indexName in indexes) {
      tasks.push(promisify(indexes[indexName].close)())
    }
    return Promise.all(tasks).then(cb)
  }

  // override query() from jitdb to implicitly call fromDB()
  function query(first, ...rest) {
    const waitForDrain = and(deferred((meta, cb) => log.onDrain(cb)))
    if (!first.meta) {
      const ops = fromDB(jitdb)
      ops.meta.db2 = this
      return jitdbOperators.query(ops, waitForDrain, first, ...rest)
    } else {
      return jitdbOperators.query(first, waitForDrain, ...rest)
    }
  }

  return {
    // API:
    get,
    add,
    publish,
    query,
    del,
    deleteFeed,
    validateAndAdd,
    validateAndAddOOO,
    getStatus,

    // needed primarily internally by other plugins in this project:
    post,
    getLatest: baseIndex.getLatest,
    getAllLatest: baseIndex.getAllLatest,
    getLog: () => log,
    registerIndex,
    getIndexes: () => indexes,
    clearIndexes,
    onDrain,
    getJITDB: () => jitdb,
  }
}
