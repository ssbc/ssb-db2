// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const os = require('os')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const clarify = require('clarify-error')
const push = require('push-stream')
const Notify = require('pull-notify')
const ssbKeys = require('ssb-keys')
const validate = require('ssb-validate') // TODO: remove this eventually
const validate2 =
  typeof localStorage === 'undefined' || localStorage === null
    ? require('ssb-validate2-rsjs-node')
    : require('ssb-validate2')
const SSBURI = require('ssb-uri2')
const bipf = require('bipf')
const pull = require('pull-stream')
const paramap = require('pull-paramap')
const Ref = require('ssb-ref')
const Obv = require('obz')
const promisify = require('promisify-4loc')
const jitdbOperators = require('jitdb/operators')
const bendyButt = require('ssb-bendy-butt')
const JITDb = require('jitdb')
const Debug = require('debug')
const multicb = require('multicb')
const mutexify = require('mutexify')

const operators = require('./operators')
const { indexesPath } = require('./defaults')
const { onceWhen } = require('./utils')
const DebouncingBatchAdd = require('./debounce-batch')
const Log = require('./log')
const Status = require('./status')
const makeBaseIndex = require('./indexes/base')
const KeysIndex = require('./indexes/keys')
const PrivateIndex = require('./indexes/private')

const { where, fromDB, key, author, deferred, toCallback, asOffsets } =
  operators

exports.name = 'db'

exports.version = '1.9.1'

exports.manifest = {
  get: 'async',
  add: 'async',
  publish: 'async',
  publishAs: 'async',
  del: 'async',
  deleteFeed: 'async',
  addTransaction: 'async',
  addOOO: 'async',
  addBatch: 'async',
  addOOOBatch: 'async',
  getStatus: 'sync',
  indexingProgress: 'source',

  // `query` should be `sync`, but secret-stack is automagically converting it
  // to async because of secret-stack/utils.js#hookOptionalCB. Eventually we
  // should include an option `synconly` in secret-stack that bypasses the hook,
  // but for now we leave the `query` API *implicitly* available in the plugin:

  // query: 'sync',
}

exports.init = function (sbot, config) {
  let self
  let closed = false
  config = config || {}
  config.db2 = config.db2 || {}
  if (config.temp) {
    const temp = typeof config.temp === 'string' ? config.temp : '' + Date.now()
    config.path = path.join(os.tmpdir(), temp)
    rimraf.sync(config.path)
    mkdirp.sync(config.path)
  }
  const indexes = {}
  const dir = config.path
  const privateIndex = PrivateIndex(dir, sbot, config)
  const log = Log(dir, config, privateIndex)
  const jitdb = JITDb(log, indexesPath(dir))
  const status = Status(log, jitdb)
  const debug = Debug('ssb:db2')
  const post = Obv()
  const indexingProgress = Notify()
  const hmacKey = null
  const stateFeedsReady = Obv().set(false)
  const state = {}

  sbot.close.hook(function (fn, args) {
    close((err) => {
      fn.apply(this, args)
    })
  })

  registerIndex(makeBaseIndex(privateIndex))
  registerIndex(KeysIndex)

  loadStateFeeds()

  function setStateFeedsReady(x) {
    stateFeedsReady.set(x)
  }

  function loadStateFeeds(cb) {
    // restore current state
    validate2.ready(() => {
      onDrain('base', () => {
        pull(
          indexes.base.getAllLatest(),
          paramap((latest, cb) => {
            getMsgByOffset(latest.value.offset, (err, kvt) => {
              if (err) cb(err)
              else cb(null, kvt)
            })
          }, 8),
          pull.collect((err, kvts) => {
            if (err) return console.error(clarify(err, 'loadStateFeeds failed'))
            for (const kvt of kvts) {
              updateState(kvt)
            }
            debug('getAllLatest is done setting up initial validate state')
            if (!stateFeedsReady.value) stateFeedsReady.set(true)
            if (cb) cb()
          })
        )
      })
    })
  }

  function updateState(kvt) {
    state[kvt.value.author] = PrivateIndex.reEncrypt(kvt)
  }

  status.obv((stats) => {
    indexingProgress(stats.progress)
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

  function getHelper(id, onlyValue, cb) {
    self.query(
      where(key(id)),
      toCallback((err, results) => {
        if (err) return cb(clarify(err, 'ssb-db2 failed to get message'))
        else if (results.length)
          return cb(null, onlyValue ? results[0].value : results[0])
        else return cb(new Error('Key not found in database ' + id))
      })
    )
  }

  function get(id, cb) {
    getHelper(id, true, cb)
  }

  function getMsg(id, cb) {
    getHelper(id, false, cb)
  }

  function getMsgByOffset(offset, cb) {
    log.get(offset, (err, buf) => {
      if (err) return cb(err)
      cb(null, bipf.decode(buf, 0))
    })
  }

  const debouncePeriod = config.db2.addDebounce || 250
  const debouncer = new DebouncingBatchAdd(addBatch, debouncePeriod)

  function addOOOBatch(msgVals, cb) {
    const guard = guardAgainstDuplicateLogs('addOOOBatch()')
    if (guard) return cb(guard)

    onceWhen(
      stateFeedsReady,
      (ready) => ready === true,
      () => {
        validate2.validateOOOBatch(hmacKey, msgVals, (err, keys) => {
          if (err) return cb(clarify(err, 'validation in addOOOBatch() failed'))

          const done = multicb({ pluck: 1 })
          for (var i = 0; i < msgVals.length; ++i)
            log.add(keys[i], msgVals[i], done())

          done(cb)
        })
      }
    )
  }

  function addTransaction(msgVals, oooMsgVals, cb) {
    const guard = guardAgainstDuplicateLogs('addTransaction()')
    if (guard) return cb(guard)

    oooMsgVals = oooMsgVals || []
    msgVals = msgVals || []

    onceWhen(
      stateFeedsReady,
      (ready) => ready === true,
      () => {
        const done = multicb({ pluck: 1 })

        if (msgVals.length > 0) {
          const author = msgVals[0].author
          if (!Ref.isFeedId(author))
            return cb(
              new Error('addTransaction() does not support feed ID ' + author)
            )

          const latestMsgVal = state[author] ? state[author].value : null
          validate2.validateBatch(hmacKey, msgVals, latestMsgVal, done())
        } else {
          done()(null, [])
        }

        validate2.validateOOOBatch(hmacKey, oooMsgVals, done())

        done((err, keys) => {
          if (err) return cb(clarify(err, 'validation in addTransaction() failed')) // prettier-ignore

          const [msgKeys, oooKeys] = keys

          if (msgKeys.length > 0) {
            const lastIndex = msgKeys.length - 1
            updateState({
              key: msgKeys[lastIndex],
              value: msgVals[lastIndex],
            })
          }

          log.addTransaction(
            msgKeys.concat(oooKeys),
            msgVals.concat(oooMsgVals),
            (err, kvts) => {
              if (err) return cb(clarify(err, 'addTransaction() failed in the log')) // prettier-ignore

              kvts.forEach((kvt) => post.set(kvt))
              cb(null, kvts)
            }
          )
        })
      }
    )
  }

  function add(msgVal, cb) {
    const guard = guardAgainstDuplicateLogs('add()')
    if (guard) return cb(guard)

    onceWhen(
      stateFeedsReady,
      (ready) => ready === true,
      () => {
        if (Ref.isFeedId(msgVal.author)) {
          debouncer.add(msgVal, cb)
        } else if (SSBURI.isBendyButtV1FeedSSBURI(msgVal.author)) {
          addImmediately(msgVal, cb)
        } else {
          cb(new Error('Unknown feed format: ' + msgVal.author))
        }
      }
    )
  }

  function addBatch(msgVals, cb) {
    const guard = guardAgainstDuplicateLogs('addBatch()')
    if (guard) return cb(guard)

    if (msgVals.length === 0) {
      return cb(null, [])
    }
    const author = msgVals[0].author
    if (!Ref.isFeedId(author)) {
      return cb(new Error('addBatch() does not support feed ID ' + author))
    }

    onceWhen(
      stateFeedsReady,
      (ready) => ready === true,
      () => {
        const latestMsgVal = state[author] ? state[author].value : null
        validate2.validateBatch(hmacKey, msgVals, latestMsgVal, (err, keys) => {
          if (err) return cb(clarify(err, 'validation in addBatch() failed'))

          const done = multicb({ pluck: 1 })
          for (var i = 0; i < msgVals.length; ++i) {
            const isLast = i === msgVals.length - 1

            if (isLast) updateState({ key: keys[i], value: msgVals[i] })

            log.add(keys[i], msgVals[i], (err, kvt) => {
              if (err) return done()(clarify(err, 'addBatch() failed in the log')) // prettier-ignore

              post.set(kvt)
              done()(null, kvt)
            })
          }

          done(cb)
        })
      }
    )
  }

  function encryptContent(keys, content) {
    if (sbot.box2 && content.recps.every(sbot.box2.supportsBox2)) {
      const feedState = state[keys.id]
      return sbot.box2.encryptClassic(
        keys,
        content,
        feedState ? feedState.key : null
      )
    } else return ssbKeys.box(content, content.recps)
  }

  function addImmediately(msgVal, cb) {
    const guard = guardAgainstDuplicateLogs('addImmediately()')
    if (guard) return cb(guard)

    onceWhen(
      stateFeedsReady,
      (ready) => ready === true,
      () => {
        if (Ref.isFeedId(msgVal.author)) {
          const previous = (state[msgVal.author] || { value: null }).value
          validate2.validateSingle(hmacKey, msgVal, previous, (err, key) => {
            if (err) return cb(clarify(err, 'classic message validation in addImmediately() failed')) // prettier-ignore
            updateState({ key, value: msgVal })
            log.add(key, msgVal, (err, kvt) => {
              if (err) return cb(clarify(err, 'addImmediately() of a classic message failed in the log')) // prettier-ignore

              post.set(kvt)
              cb(null, kvt)
            })
          })
        } else if (SSBURI.isBendyButtV1FeedSSBURI(msgVal.author)) {
          const previous = (state[msgVal.author] || { value: null }).value
          const err = bendyButt.validateSingle(msgVal, previous, hmacKey)
          if (err) return cb(clarify(err, 'bendy butt message validation in addImmediately() failed')) // prettier-ignore
          const key = bendyButt.hash(msgVal)
          updateState({ key, value: msgVal })
          log.add(key, msgVal, (err, kvt) => {
            if (err) return cb(clarify(err, 'addImmediately() of a bendy butt message failed in the log')) // prettier-ignore

            post.set(kvt)
            cb(null, kvt)
          })
        } else {
          cb(new Error('Unknown feed format: ' + msgVal.author))
        }
      }
    )
  }

  function addOOO(msgVal, cb) {
    const guard = guardAgainstDuplicateLogs('addOOO()')
    if (guard) return cb(guard)

    validate2.validateOOOBatch(hmacKey, [msgVal], (err, keys) => {
      if (err) return cb(clarify(err, 'validation in addOOO() failed'))
      const key = keys[0]
      get(key, (err, data) => {
        if (data) return cb(null, data)
        log.add(key, msgVal, (err, data) => {
          if (err) return cb(clarify(err, 'addOOO() failed in the log'))
          cb(null, data)
        })
      })
    })
  }

  function publish(content, cb) {
    const guard = guardAgainstDuplicateLogs('publish()')
    if (guard) return cb(guard)

    publishAs(config.keys, content, cb)
  }

  function publishAs(keys, content, cb) {
    const guard = guardAgainstDuplicateLogs('publishAs()')
    if (guard) return cb(guard)

    if (!Ref.isFeedId(keys.id)) {
      return cb(
        new Error('publishAs() does not support feed format: ' + keys.id)
      )
    }

    onceWhen(
      stateFeedsReady,
      (ready) => ready === true,
      () => {
        if (content.recps) {
          try {
            content = encryptContent(keys, content)
          } catch (ex) {
            return cb(ex)
          }
        }
        const latestKVT = state[keys.id]
        const msgVal = validate.create(
          latestKVT ? { queue: [latestKVT] } : null,
          keys,
          hmacKey,
          content,
          Date.now()
        )
        addImmediately(msgVal, cb)
      }
    )
  }

  function del(msgId, cb) {
    const guard = guardAgainstDuplicateLogs('del()')
    if (guard) return cb(guard)

    self.query(
      where(key(msgId)),
      asOffsets(),
      toCallback((err, results) => {
        if (err) return cb(clarify(err, 'del() failed when getting the message')) // prettier-ignore
        if (results.length === 0) return cb(new Error(`cannot delete ${msgId} because it was not found`)) // prettier-ignore

        indexes['keys'].delMsg(msgId)
        log.del(results[0], cb)
      })
    )
  }

  function deleteFeed(feedId, cb) {
    const guard = guardAgainstDuplicateLogs('deleteFeed()')
    if (guard) return cb(guard)

    jitdb.all(author(feedId), 0, false, true, 'declared', (err, offsets) => {
      push(
        push.values(offsets),
        push.asyncMap((offset, cb) => {
          log.del(offset, cb)
        }),
        push.collect((err) => {
          if (err) cb(clarify(err, 'deleteFeed() failed for feed ' + feedId))
          else {
            delete state[feedId]
            indexes.base.removeFeedFromLatest(feedId, cb)
          }
        })
      )
    })
  }

  function clearIndexes() {
    for (const indexName in indexes) indexes[indexName].remove(() => {})
  }

  function registerIndex(Index) {
    const index = new Index(log, dir)

    if (indexes[index.name]) throw 'Index already exists'

    index.offset((o) => status.updateIndex(index.name, o))

    indexes[index.name] = index
  }

  function updateIndexes() {
    const start = Date.now()

    const indexesArr = Object.values(indexes)

    const lowestOffset = Math.min(
      ...indexesArr.map((idx) => idx.offset.value),
      privateIndex.latestOffset.value
    )
    debug(`lowest offset for all indexes is ${lowestOffset}`)

    log.stream({ gt: lowestOffset }).pipe({
      paused: false,
      write(record) {
        indexesArr.forEach((idx) => idx.onRecord(record, false))
      },
      end() {
        debug(`updateIndexes() scan time: ${Date.now() - start}ms`)
        const writeTasks = indexesArr.map((idx) =>
          promisify(idx.flush.bind(idx))()
        )
        Promise.all(writeTasks).then(() => {
          debug('updateIndexes() live streaming')
          log.stream({ gt: indexes['base'].offset.value, live: true }).pipe({
            paused: false,
            write(record) {
              indexesArr.forEach((idx) => idx.onRecord(record, true))
            },
          })
        })
      },
    })
  }

  function onDrain(indexName, cb) {
    if (closed) return
    if (!cb) {
      // default
      cb = indexName
      indexName = 'base'
    }

    // setTimeout to make sure extra indexes from secret-stack are also included
    setTimeout(() => {
      if (closed) return
      onIndexesStateLoaded(() => {
        if (closed) return
        log.onDrain(() => {
          if (closed) return
          const index = indexes[indexName]
          if (!index) return cb('Unknown index:' + indexName)

          status.updateLog()

          if (index.offset.value === log.since.value) {
            status.updateIndex(indexName, index.offset.value)
            cb()
          } else {
            const remove = index.offset(() => {
              if (closed) return
              if (index.offset.value === log.since.value) {
                remove()
                status.updateIndex(indexName, index.offset.value)
                cb()
              }
            })
          }
        })
      })
    })
  }

  function onIndexesStateLoaded(cb) {
    if (!onIndexesStateLoaded.promise) {
      const stateLoadedPromises = [privateIndex.stateLoaded]
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
    closed = true
    const tasks = []
    for (const indexName in indexes) {
      const index = indexes[indexName]
      tasks.push(promisify(index.close.bind(index))())
    }
    Promise.all(tasks)
      .then(() => promisify(log.close)())
      .then(cb, cb)
  }

  // override query() from jitdb to implicitly call fromDB()
  function query(first, ...rest) {
    // Before running the query, the log needs to be migrated/synced with the
    // old log and it should be 'drained'
    const waitUntilReady = deferred((meta, cb) => {
      if (sbot.db2migrate) {
        sbot.db2migrate.synchronized((isSynced) => {
          if (isSynced) onDrain(cb)
        })
      } else {
        onDrain(cb)
      }
    })

    if (first && first.meta) {
      return jitdbOperators.query(first, where(waitUntilReady), ...rest)
    } else {
      const ops = fromDB(jitdb)
      ops.meta.db = this
      return jitdbOperators.query(ops, where(waitUntilReady), first, ...rest)
    }
  }

  function prepare(operation, cb) {
    if (sbot.db2migrate) {
      sbot.db2migrate.synchronized((isSynced) => {
        if (isSynced) next()
      })
    } else {
      next()
    }

    function next() {
      jitdb.prepare(operation, cb)
    }
  }

  function reindexOffset(data, cb) {
    jitdb.reindex(data.offset, (err) => {
      if (err) return cb(clarify(err, 'reindexOffset() failed'))

      for (const indexName in indexes) {
        const idx = indexes[indexName]
        if (idx.indexesContent()) idx.processRecord(data, data.seq)
      }

      cb(null, data.offset)
    })
  }

  const reindexingLock = mutexify()

  function reindexEncrypted(cb) {
    reindexingLock((unlock) => {
      const offsets = privateIndex.missingDecrypt()
      const keysIndex = indexes['keys']
      const B_KEY = Buffer.from('key')
      const B_META = Buffer.from('meta')
      const B_PRIVATE = Buffer.from('private')

      push(
        push.values(offsets),
        push.asyncMap((offset, cb) => {
          log.get(offset, (err, buf) => {
            if (err) return cb(clarify(err, 'reindexEncrypted() failed when getting messages')) // prettier-ignore

            const pMeta = bipf.seekKey(buf, 0, B_META)
            if (pMeta < 0) return cb()
            const pPrivate = bipf.seekKey(buf, pMeta, B_PRIVATE)
            if (pPrivate < 0) return cb()

            // check if we can decrypt the record
            if (!bipf.decode(buf, pPrivate)) return cb()

            const pKey = bipf.seekKey(buf, 0, B_KEY)
            if (pKey < 0) return cb()
            const key = bipf.decode(buf, pKey)

            onDrain('keys', () => {
              keysIndex.getSeq(key, (err, seqNum) => {
                if (err) return cb(clarify(err, 'reindexEncrypted() failed when getting seq')) // prettier-ignore

                const seq = parseInt(seqNum, 10)

                reindexOffset({ offset, seq, value: buf }, cb)
              })
            })
          })
        }),
        push.collect((err, result) => {
          unlock(cb, err, result)
        })
      )
    })
  }

  return (self = {
    // Public API:
    get,
    getMsg,
    query,
    prepare,
    del,
    deleteFeed,
    add,
    publish,
    publishAs,
    addTransaction,
    addOOO,
    addOOOBatch,
    getStatus: () => status.obv,
    operators,
    post,
    reindexEncrypted,
    indexingProgress: () => indexingProgress.listen(),

    // used for partial replication in browser, will be removed soon!
    setPost: post.set,

    // needed primarily internally by other plugins in this project:
    addBatch,
    addImmediately,
    getLatest: indexes.base.getLatest.bind(indexes.base),
    getAllLatest: indexes.base.getAllLatest.bind(indexes.base),
    getLog: () => log,
    registerIndex,
    setStateFeedsReady,
    loadStateFeeds,
    stateFeedsReady,
    getState: () => state,
    getIndexes: () => indexes,
    getIndex: (index) => indexes[index],
    clearIndexes,
    onDrain,
    getJITDB: () => jitdb,
  })
}
