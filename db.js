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

exports.version = '0.12.0'

exports.manifest = {
  get: 'async',
  add: 'async',
  publish: 'async',
  del: 'async',
  deleteFeed: 'async',
  addOOO: 'async',
  addOOOStrictOrder: 'async',
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

  /**
   * Beware:
   * There is a race condition here if you add the same message quickly
   * after another because baseIndex is lazy. The default js SSB
   * implementation adds messages in order, so it doesn't really have
   * this problem.
   */
  function rawAdd(msg, cb) {
    stateFeedsReady.promise.then(() => {
      const id = getId(msg)
      get(id, (err, data) => {
        if (data) cb(null, data)
        else log.add(id, msg, cb)
      })
    })
  }

  function add(msg, cb) {
    const guard = guardAgainstDuplicateLogs('add()')
    if (guard) return cb(guard)

    try {
      state = validate.append(state, hmac_key, msg)
      if (state.error) return cb(state.error)
      rawAdd(msg, cb)
    } catch (ex) {
      return cb(ex)
    }
  }

  function addOOO(msg, cb) {
    const guard = guardAgainstDuplicateLogs('addOOO()')
    if (guard) return cb(guard)

    try {
      let oooState = validate.initial()
      validate.appendOOO(oooState, hmac_key, msg)

      if (oooState.error) return cb(oooState.error)

      rawAdd(msg, cb)
    } catch (ex) {
      return cb(ex)
    }
  }

  function addOOOStrictOrder(msg, cb) {
    const guard = guardAgainstDuplicateLogs('addOOOStrictOrder()')
    if (guard) return cb(guard)

    const knownAuthor = msg.author in state.feeds

    try {
      if (!knownAuthor) state = validate.appendOOO(state, hmac_key, msg)
      else state = validate.append(state, hmac_key, msg)

      if (state.error) return cb(state.error)

      rawAdd(msg, cb)
    } catch (ex) {
      return cb(ex)
    }
  }

  function publish(msg, cb) {
    const guard = guardAgainstDuplicateLogs('publish()')
    if (guard) return cb(guard)

    state.queue = []
    state = validate.appendNew(state, null, config.keys, msg, Date.now())
    rawAdd(state.queue[0].value, (err, data) => {
      post.set(data)
      cb(err, data)
    })
  }

  function del(msgId, cb) {
    const guard = guardAgainstDuplicateLogs('del()')
    if (guard) return cb(guard)

    jitdb.all(key(msgId), 0, false, true, (err, results) => {
      if (err) return cb(err)
      if (results.length === 0)
        return cb(
          new Error('cannot delete ' + msgId + ' because it was not found')
        )

      log.del(results[0], cb)
    })
  }

  function deleteFeed(feedId, cb) {
    const guard = guardAgainstDuplicateLogs('deleteFeed()')
    if (guard) return cb(guard)

    jitdb.all(author(feedId), 0, false, true, (err, offsets) => {
      push(
        push.values(offsets),
        push.asyncMap((offset, cb) => {
          log.del(offset, cb)
        }),
        push.collect((err) => {
          if (err) cb(err)
          else {
            delete state.feeds[feedId]
            baseIndex.removeFeedFromLatest(feedId)
            cb()
          }
        })
      )
    })
  }

  function getStatus() {
    const result = {
      log: log.since.value,
      indexes: {},
    }

    for (const indexName in indexes)
      result.indexes[indexName] = indexes[indexName].offset.value

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

    const indexesArr = Object.values(indexes)

    const lowestOffset = Math.min(...indexesArr.map((idx) => idx.offset.value))
    debug(`lowest offset for all indexes is ${lowestOffset}`)

    log.stream({ gt: lowestOffset }).pipe({
      paused: false,
      write(data) {
        indexesArr.forEach((idx) => idx.onData(data, false))
      },
      end() {
        debug(`updateIndexes() scan time: ${Date.now() - start}ms`)
        const writeTasks = indexesArr.map((idx) => promisify(idx.writeBatch)())
        Promise.all(writeTasks).then(() => {
          debug('updateIndexes() live streaming')
          log.stream({ gt: indexes['base'].offset.value, live: true }).pipe({
            paused: false,
            write(data) {
              indexesArr.forEach((idx) => idx.onData(data, true))
            },
          })
        })
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

        if (index.offset.value === log.since.value) {
          cb()
        } else {
          const remove = index.offset(() => {
            if (index.offset.value === log.since.value) {
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
      for (const indexName in indexes) {
        stateLoadedPromises.push(indexes[indexName].stateLoaded)
      }
      onIndexesStateLoaded.promise = Promise.all(stateLoadedPromises)
    }
    onIndexesStateLoaded.promise.then(cb)
  }

  // setTimeout to make sure extra indexes from secret-stack are also included
  const timer = setTimeout(() => {
    onIndexesStateLoaded(updateIndexes)
  })
  if (timer.unref) timer.unref()

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
    query,
    del,
    deleteFeed,
    add,
    publish,
    addOOO,
    addOOOStrictOrder,
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
