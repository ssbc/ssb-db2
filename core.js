// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const os = require('os')
const fs = require('fs')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const Debug = require('debug')
const multicb = require('multicb')
const mutexify = require('mutexify')
const Notify = require('pull-notify')
const pull = require('pull-stream')
const paramap = require('pull-paramap')
const Obv = require('obz')
const bipf = require('bipf')
const JITDB = require('jitdb')
const jitdbOperators = require('jitdb/operators')
const uri = require('ssb-uri2')

const operators = require('./operators')
const {
  jitIndexesPath,
  resetLevelPath,
  resetPrivatePath,
  reindexEncryptedInProgressPath,
} = require('./defaults')
const { onceWhen, ReadyGate, onceWhenPromise } = require('./utils')
const ThrottleBatchAdd = require('./throttle-batch')
const Log = require('./log')
const Status = require('./status')
const makeBaseIndex = require('./indexes/base')
const KeysIndex = require('./indexes/keys')
const PrivateIndex = require('./indexes/private')

const BIPF_VALUE = bipf.allocAndEncode('value')
const BIPF_KEY = bipf.allocAndEncode('key')
const BIPF_META = bipf.allocAndEncode('meta')
const BIPF_PRIVATE = bipf.allocAndEncode('private')

const {
  where,
  fromDB,
  author,
  deferred,
  asOffsets,
  isEncrypted,
  batch,
  toPullStream,
} = operators
const isBrowser = typeof window !== 'undefined'

exports.name = 'db'

exports.version = '1.9.1'

exports.manifest = {
  get: 'async',
  add: 'async',
  create: 'async',
  publish: 'async',
  publishAs: 'async',
  del: 'async',
  deleteFeed: 'async',
  addTransaction: 'async',
  addOOO: 'async',
  addBatch: 'async',
  addOOOBatch: 'async',
  getStatus: 'sync',
  compact: 'async',
  logStats: 'async',
  indexingProgress: 'source',
  compactionProgress: 'source',
  reset: 'async',

  // `query` should be `sync`, but secret-stack is automagically converting it
  // to async because of secret-stack/utils.js#hookOptionalCB. Eventually we
  // should include an option `synconly` in secret-stack that bypasses the hook,
  // but for now we leave the `query` API *implicitly* available in the plugin:

  // query: 'sync',
}

exports.init = function (sbot, config) {
  const self = {}
  let closed = false
  config = config || {}
  config.db2 = config.db2 || {}
  if (config.temp) {
    const temp = typeof config.temp === 'string' ? config.temp : '' + Date.now()
    config.path = path.join(os.tmpdir(), temp)
    rimraf.sync(config.path)
    mkdirp.sync(config.path)
  }
  const feedFormats = []
  const encryptionFormats = []
  const indexes = {}
  const dir = config.path
  const privateIndex = PrivateIndex(dir, sbot, config)
  const log = Log(dir, config, privateIndex, self)
  const jitdb = JITDB(log, jitIndexesPath(dir))
  const status = Status(log, jitdb)
  const debug = Debug('ssb:db2')
  const onMsgAdded = Obv()
  const indexingProgress = Notify()
  const compactionProgress = Notify()
  const indexingActive = Obv().set(0)
  let abortLogStreamForIndexes = null
  const compacting = Obv().set(!log.compactionProgress.value.done)
  const hmacKey = null
  const stateFeedsReady = Obv().set(false)
  const secretStackLoaded = new ReadyGate()
  const indexesStateLoaded = new ReadyGate()
  const reindexingLock = mutexify()
  const reindexedValues = Notify()

  sbot.close.hook(function (fn, args) {
    close((err) => {
      fn.apply(this, args)
    })
  })

  privateIndex.latestOffset((offset) => status.updateIndex('private', offset))
  registerIndex(makeBaseIndex(privateIndex))
  registerIndex(KeysIndex)

  loadStateFeeds()

  // Wait a bit for other secret-stack plugins (which may add indexes) to load
  const secretStackTimer = setTimeout(() => {
    secretStackLoaded.setReady()
  }, 16)
  if (secretStackTimer.unref) secretStackTimer.unref()

  secretStackLoaded.onReady(() => {
    const stateLoadedPromises = [privateIndex.stateLoaded]
    for (const indexName in indexes) {
      stateLoadedPromises.push(indexes[indexName].stateLoaded)
    }
    Promise.all(stateLoadedPromises).then(() => {
      if (!isBrowser && fs.existsSync(reindexEncryptedInProgressPath(dir))) {
        reindexEncrypted(() => {
          debug('done reindexing encrypted from a previous session')
        })
      }
      indexesStateLoaded.setReady()
    })
  })

  indexesStateLoaded.onReady(updateIndexes)

  function setStateFeedsReady(x) {
    stateFeedsReady.set(x)
  }

  const state = {
    _map: new Map(), // feedId => nativeMsg
    updateFromKVT(kvtf) {
      const feedId = kvtf.feed || kvtf.value.author
      const feedFormat = findFeedFormatForAuthor(feedId)
      if (!feedFormat) {
        console.warn('No feed format installed understands ' + feedId)
        return
      }
      const nativeMsg = feedFormat.toNativeMsg(kvtf.value, 'js')
      this._map.set(feedId, nativeMsg)
    },
    update(feedId, nativeMsg) {
      this._map.set(feedId, nativeMsg)
    },
    get(feedId) {
      return this._map.get(feedId) || null
    },
    has(feedId) {
      return this._map.has(feedId)
    },
    getAsKV(feedId, feedFormat) {
      const nativeMsg = this._map.get(feedId)
      if (!nativeMsg) return null
      const feedFormat2 = feedFormat || findFeedFormatForAuthor(feedId)
      if (!feedFormat2) {
        throw new Error('No feed format installed understands ' + feedId)
      }
      const key = feedFormat2.getMsgId(nativeMsg, 'js')
      const value = feedFormat2.fromNativeMsg(nativeMsg, 'js')
      return { key, value }
    },
    delete(feedId) {
      this._map.delete(feedId)
    },
  }

  function loadStateFeeds(cb) {
    onDrain('base', () => {
      pull(
        indexes.base.getAllLatest(),
        paramap((latest, cb) => {
          getMsgByOffset(latest.value.offset, (err, kvt) => {
            if (err) cb(err)
            else cb(null, kvt)
          })
        }, 8),
        pull.drain(
          (kvt) => {
            state.updateFromKVT(PrivateIndex.reEncrypt(kvt))
          },
          (err) => {
            // prettier-ignore
            if (err) return console.error(new Error('loadStateFeeds failed', {cause: err}))
            debug('getAllLatest is done setting up initial validate state')
            if (!stateFeedsReady.value) stateFeedsReady.set(true)
            if (cb) cb()
          }
        )
      )
    })
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
    id = uri.isClassicMessageSSBURI(id) ? uri.toMessageSigil(id) : id

    if (sbot.db2migrate) {
      onceWhen(
        sbot.db2migrate.synchronized,
        (isSynced) => isSynced === true,
        () => onDrain('keys', next)
      )
    } else {
      onDrain('keys', next)
    }

    function next() {
      indexes['keys'].getSeq(id, (err, seq) => {
        // prettier-ignore
        if (err) cb(new Error('Msg ' + id + ' not found in leveldb index', {cause: err}))
        else {
          jitdb.lookup('seq', seq, (err, offset) => {
            // prettier-ignore
            if (err) cb(new Error('Msg ' + id + ' not found in jit index', {cause: err}))
            else {
              getMsgByOffset(offset, (err, msg) => {
                // prettier-ignore
                if (err) cb(new Error('Msg ' + id + ' not found in the log', {cause: err}))
                else cb(null, onlyValue ? msg.value : msg)
              })
            }
          })
        }
      })
    }
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

  function findFeedFormatForNativeMsg(nativeMsg) {
    for (const feedFormat of feedFormats) {
      if (feedFormat.isNativeMsg(nativeMsg)) return feedFormat
    }
    return null
  }

  function findFeedFormatForAuthor(author) {
    for (const feedFormat of feedFormats) {
      if (feedFormat.isAuthor(author)) return feedFormat
    }
    return null
  }

  function findFeedFormatByName(formatName) {
    for (const feedFormat of feedFormats) {
      if (feedFormat.name === formatName) return feedFormat
    }
    return null
  }

  function findFeedFormatByNameOrNativeMsg(formatName, nativeMsg) {
    if (formatName) {
      const feedFormat = findFeedFormatByName(formatName)
      if (feedFormat) return feedFormat
    }
    return findFeedFormatForNativeMsg(nativeMsg)
  }

  function installFeedFormat(feedFormat) {
    if (!feedFormat.encodings.includes('js')) {
      // prettier-ignore
      throw new Error('ssb-db2: feed format ' + feedFormat.name + ' must support js encoding')
    }
    feedFormats.push(feedFormat)
  }

  function installEncryptionFormat(encryptionFormat) {
    if (encryptionFormat.setup) {
      const loaded = new ReadyGate()
      encryptionFormat.setup(config, (err) => {
        if (err) throw err
        loaded.setReady()
      })
      encryptionFormat.onReady = loaded.onReady.bind(loaded)
    }
    encryptionFormats.push(encryptionFormat)
  }

  function findEncryptionFormatFor(ciphertextJS) {
    if (!ciphertextJS) return null
    if (typeof ciphertextJS !== 'string') return null
    for (const encryptionFormat of encryptionFormats) {
      if (ciphertextJS.endsWith(`.${encryptionFormat.name}`)) {
        return encryptionFormat
      }
    }
    return null
  }

  function findEncryptionFormatByName(formatName) {
    for (const encryptionFormat of encryptionFormats) {
      if (encryptionFormat.name === formatName) return encryptionFormat
    }
    return null
  }

  const throttlePeriod = config.db2.addBatchThrottle || 250
  const throttler = new ThrottleBatchAdd(addBatch, throttlePeriod)

  function normalizeAddArgs(...args) {
    let cb, opts
    if (typeof args[0] === 'function') {
      opts = { encoding: 'js' }
      cb = args[0]
    } else if (!args[0] && typeof args[1] === 'function') {
      opts = { encoding: 'js' }
      cb = args[1]
    } else if (typeof args[0] === 'object' && typeof args[1] === 'function') {
      opts = { encoding: 'js', ...args[0] }
      cb = args[1]
    } else {
      throw new Error('ssb-db2: invalid arguments to add')
    }
    return [opts, cb]
  }

  function add(nativeMsg, ...args) {
    const guard = guardAgainstDuplicateLogs('add()')
    if (guard) return cb(guard)
    const [opts, cb] = normalizeAddArgs(...args)

    onceWhen(
      stateFeedsReady,
      (ready) => ready === true,
      () => {
        const feedFormat = findFeedFormatByNameOrNativeMsg(
          opts.feedFormat,
          nativeMsg
        )
        if (!feedFormat) {
          // prettier-ignore
          return cb(new Error('add() failed because feed format is unknown for: ' + nativeMsg))
        }
        opts.feedFormat = feedFormat.name
        opts.feedId = feedFormat.getFeedId(nativeMsg)
        if (feedFormat.validateBatch) throttler.add(nativeMsg, opts, cb)
        else addImmediately(nativeMsg, feedFormat, opts, cb)
      }
    )
  }

  function addBatch(nativeMsgs, ...args) {
    const guard = guardAgainstDuplicateLogs('addBatch()')
    if (guard) return cb(guard)
    const [opts, cb] = normalizeAddArgs(...args)
    if (nativeMsgs.length === 0) {
      return cb(null, [])
    }
    const feedFormat = findFeedFormatByNameOrNativeMsg(
      opts.feedFormat,
      nativeMsgs[0]
    )
    if (!feedFormat) {
      return cb(
        new Error(
          'addBatch() does not support feed format for ' + nativeMsgs[0]
        )
      )
    }
    if (!feedFormat.validateBatch) {
      // prettier-ignore
      return cb(new Error('addBatch() failed because feed format ' + feedFormat.name + ' does not support validateBatch'))
    }

    onceWhen(
      stateFeedsReady,
      (ready) => ready === true,
      () => {
        const feedId = feedFormat.getFeedId(nativeMsgs[0])
        const prevNativeMsg = state.get(feedId)
        feedFormat.validateBatch(nativeMsgs, prevNativeMsg, hmacKey, (err) => {
          // prettier-ignore
          if (err) return cb(new Error('validation in addBatch() failed', {cause: err}))
          const done = multicb({ pluck: 1 })
          for (var i = 0; i < nativeMsgs.length; ++i) {
            const nativeMsg = nativeMsgs[i]
            const msgId = feedFormat.getMsgId(nativeMsg)
            const msg = feedFormat.fromNativeMsg(nativeMsg, opts.encoding)
            const isLast = i === nativeMsgs.length - 1
            if (isLast) state.update(feedId, nativeMsg)

            log.add(msgId, msg, feedId, opts.encoding, false, (err, kvt) => {
              // prettier-ignore
              if (err) return done()(new Error('addBatch() failed in the log', {cause: err}))

              onMsgAdded.set({
                kvt,
                nativeMsg,
                feedFormat: feedFormat.name,
              })
              done()(null, kvt)
            })
          }

          done(cb)
        })
      }
    )
  }

  function addImmediately(nativeMsg, feedFormat, opts, cb) {
    const feedId = feedFormat.getFeedId(nativeMsg)
    const prevNativeMsg = state.get(feedId)
    feedFormat.validate(nativeMsg, prevNativeMsg, hmacKey, (err) => {
      // prettier-ignore
      if (err) return cb(new Error('addImmediately() failed validation for feed format ' + feedFormat.name, {cause: err}))
      const msgId = feedFormat.getMsgId(nativeMsg)
      const msg = feedFormat.fromNativeMsg(nativeMsg, opts.encoding)
      state.update(feedId, nativeMsg)

      log.add(msgId, msg, feedId, opts.encoding, false, (err, kvt) => {
        // prettier-ignore
        if (err) return cb(new Error('addImmediately() failed in the log', {cause: err}))

        onMsgAdded.set({
          kvt,
          nativeMsg,
          feedFormat: feedFormat.name,
        })
        cb(null, kvt)
      })
    })
  }

  function addOOO(nativeMsg, ...args) {
    const guard = guardAgainstDuplicateLogs('addOOO()')
    if (guard) return cb(guard)
    const [opts, cb] = normalizeAddArgs(...args)
    const feedFormat = findFeedFormatByNameOrNativeMsg(
      opts.feedFormat,
      nativeMsg
    )
    if (!feedFormat) {
      // prettier-ignore
      return cb(new Error('addOOO() failed because could not find feed format for: ' + nativeMsg))
    }
    if (!feedFormat.validateOOO) {
      // prettier-ignore
      return cb(new Error('addOOO() failed because feed format ' + feedFormat.name + ' does not support validateOOO'))
    }

    feedFormat.validateOOO(nativeMsg, hmacKey, (err) => {
      // prettier-ignore
      if (err) return cb(new Error('addOOO() failed validation for feed format ' + feedFormat.name, {cause: err}))
      const msgId = feedFormat.getMsgId(nativeMsg)
      get(msgId, (err, data) => {
        if (data) return cb(null, data)
        const msg = feedFormat.fromNativeMsg(nativeMsg, opts.encoding)
        const feedId = feedFormat.getFeedId(nativeMsg)

        log.add(msgId, msg, feedId, opts.encoding, true, (err, data) => {
          // prettier-ignore
          if (err) return cb(new Error('addOOO() failed in the log', {cause: err}))
          cb(null, data)
        })
      })
    })
  }

  function addOOOBatch(nativeMsgs, ...args) {
    const guard = guardAgainstDuplicateLogs('addOOOBatch()')
    if (guard) return cb(guard)
    const [opts, cb] = normalizeAddArgs(...args)
    if (nativeMsgs.length === 0) {
      return cb(null, [])
    }
    const feedFormat = findFeedFormatByNameOrNativeMsg(
      opts.feedFormat,
      nativeMsgs[0]
    )
    if (!feedFormat) {
      // prettier-ignore
      return cb(new Error('addOOOBatch() failed because could not find feed format for: ' + nativeMsgs[0]))
    }
    if (!feedFormat.validateOOOBatch) {
      // prettier-ignore
      return cb(new Error('addOOOBatch() failed because feed format ' + feedFormat.name + ' does not support validateOOOBatch'))
    }

    feedFormat.validateOOOBatch(nativeMsgs, hmacKey, (err) => {
      // prettier-ignore
      if (err) return cb(new Error('validation in addOOOBatch() failed', {cause: err}))
      const done = multicb({ pluck: 1 })
      for (var i = 0; i < nativeMsgs.length; ++i) {
        const msgId = feedFormat.getMsgId(nativeMsgs[i])
        const msg = feedFormat.fromNativeMsg(nativeMsgs[i], opts.encoding)
        const feedId = feedFormat.getFeedId(nativeMsgs[i])
        log.add(msgId, msg, feedId, opts.encoding, true, done())
      }

      done(cb)
    })
  }

  function addTransaction(nativeMsgs, oooNativeMsgs, ...args) {
    const guard = guardAgainstDuplicateLogs('addTransaction()')
    if (guard) return cb(guard)
    const [opts, cb] = normalizeAddArgs(...args)
    oooNativeMsgs = oooNativeMsgs || []
    nativeMsgs = nativeMsgs || []
    if (nativeMsgs.length === 0 && oooNativeMsgs.length === 0) {
      return cb(null, [])
    }
    const feedFormat =
      nativeMsgs.length > 0
        ? findFeedFormatByNameOrNativeMsg(opts.feedFormat, nativeMsgs[0])
        : null
    const oooFeedFormat =
      oooNativeMsgs.length > 0
        ? findFeedFormatForNativeMsg(oooNativeMsgs[0])
        : null
    if (feedFormat && !feedFormat.validateBatch) {
      // prettier-ignore
      return cb(new Error('addTransaction() failed because feed format ' + feedFormat.name + ' does not support validateBatch'))
    }
    if (oooFeedFormat && !oooFeedFormat.validateOOOBatch) {
      // prettier-ignore
      return cb(new Error('addTransaction() failed because feed format ' + oooFeedFormat.name + ' does not support validateOOOBatch'))
    }

    onceWhen(
      stateFeedsReady,
      (ready) => ready === true,
      () => {
        const done = multicb({ pluck: 1 })

        if (nativeMsgs.length > 0) {
          const feedId = feedFormat.getFeedId(nativeMsgs[0])
          const prevNativeMsg = state.get(feedId)
          feedFormat.validateBatch(nativeMsgs, prevNativeMsg, hmacKey, done())
        } else {
          done()(null, [])
        }

        if (oooNativeMsgs.length > 0) {
          oooFeedFormat.validateOOOBatch(oooNativeMsgs, hmacKey, done())
        }

        done((err) => {
          // prettier-ignore
          if (err) return cb(new Error('validation in addTransaction() failed', {cause: err}))

          const msgIds = nativeMsgs.map((m) => feedFormat.getMsgId(m))
          const oooMsgIds = oooNativeMsgs.map((m) => oooFeedFormat.getMsgId(m))

          if (nativeMsgs.length > 0) {
            const lastIndex = nativeMsgs.length - 1
            const nativeMsg = nativeMsgs[lastIndex]
            const feedId = feedFormat.getFeedId(nativeMsg)
            state.update(feedId, nativeMsg)
          }

          const msgs = nativeMsgs.map((nMsg) =>
            feedFormat.fromNativeMsg(nMsg, opts.encoding)
          )
          const oooMsgs = oooNativeMsgs.map((nMsg) =>
            oooFeedFormat.fromNativeMsg(nMsg, opts.encoding)
          )

          log.addTransaction(
            msgIds,
            oooMsgIds,
            msgs,
            oooMsgs,
            opts.encoding,
            (err, kvts) => {
              if (err)
                // prettier-ignore
                return cb(new Error('addTransaction() failed in the log', {cause: err}))
              if (kvts.length !== msgIds.length + oooMsgIds.length) {
                // prettier-ignore
                return cb(new Error('addTransaction() failed due to mismatched message count'))
              }

              for (let i = 0; i < kvts.length; ++i) {
                const nativeMsg =
                  i < nativeMsgs.length
                    ? nativeMsgs[i]
                    : oooNativeMsgs[i - nativeMsgs.length]
                const ff =
                  i < nativeMsgs.length ? feedFormat.name : oooFeedFormat.name
                onMsgAdded.set({
                  kvt: kvts[i],
                  nativeMsg,
                  feedFormat: ff,
                })
              }
              cb(null, kvts)
            }
          )
        })
      }
    )
  }

  async function create(opts, cb) {
    const guard = guardAgainstDuplicateLogs('create()')
    if (guard) return cb(guard)

    const keys = opts.keys || config.keys

    const feedFormat = findFeedFormatByName(opts.feedFormat || 'classic')
    const encryptionFormat = findEncryptionFormatByName(
      opts.encryptionFormat || 'box'
    )
    const encoding = opts.encoding || 'js'
    // prettier-ignore
    if (!feedFormat) return cb(new Error('create() does not support feed format ' + opts.feedFormat))
    // prettier-ignore
    if (!feedFormat.encodings.includes(encoding)) return cb(new Error('create() does not support encoding ' + encoding))
    // prettier-ignore
    if (!feedFormat.isAuthor(keys.id)) return cb(new Error(`create() failed because keys.id ${keys.id} is not a valid author for feed format ${feedFormat.name}`))
    // prettier-ignore
    if (opts.recps || opts.content.recps) {
      if (!encryptionFormat) {
        return cb(new Error('create() does not support encryption format ' + opts.encryptionFormat))
      }
    }

    if (!opts.content) return cb(new Error('create() requires a `content`'))

    await privateIndex.stateLoaded
    await onceWhenPromise(stateFeedsReady, (ready) => ready === true)

    // Create full opts:

    let provisionalNativeMsg
    try {
      provisionalNativeMsg = feedFormat.newNativeMsg({
        timestamp: Date.now(),
        ...opts,
        previous: null,
        keys,
      })
    } catch (err) {
      return cb(new Error('create() failed', { cause: err }))
    }
    const feedId = feedFormat.getFeedId(provisionalNativeMsg)
    const previous = state.getAsKV(feedId, feedFormat)
    const fullOpts = { timestamp: Date.now(), ...opts, previous, keys, hmacKey }

    // If opts ask for encryption, encrypt and put ciphertext in opts.content
    const recps = fullOpts.recps || fullOpts.content.recps
    if (Array.isArray(recps) && recps.length > 0) {
      const plaintext = feedFormat.toPlaintextBuffer(fullOpts)
      const encryptOpts = {
        ...fullOpts,
        keys,
        recps,
        previous: previous ? previous.key : null,
      }
      let ciphertextBuf
      try {
        ciphertextBuf = encryptionFormat.encrypt(plaintext, encryptOpts)
      } catch (err) {
        return cb(
          new Error('create() failed to encrypt content', { cause: err })
        )
      }
      if (!ciphertextBuf) {
        // prettier-ignore
        return cb(new Error('create() failed to encrypt with ' + encryptionFormat.name))
      }
      const ciphertextBase64 = ciphertextBuf.toString('base64')
      fullOpts.content = ciphertextBase64 + '.' + encryptionFormat.name
    }

    // Create the native message:
    let nativeMsg
    try {
      nativeMsg = feedFormat.newNativeMsg(fullOpts)
    } catch (err) {
      return cb(new Error('create() failed', { cause: err }))
    }
    const msgId = feedFormat.getMsgId(nativeMsg)
    const msg = feedFormat.fromNativeMsg(nativeMsg, encoding)
    state.update(feedId, nativeMsg)

    // Encode the native message and append it to the log:
    log.add(msgId, msg, feedId, encoding, false, (err, kvt) => {
      if (err)
        return cb(new Error('create() failed in the log', { cause: err }))
      onMsgAdded.set({
        kvt,
        nativeMsg: nativeMsg,
        feedFormat: feedFormat.name,
      })
      cb(null, kvt)
    })
  }

  function del(msgId, cb) {
    const guard = guardAgainstDuplicateLogs('del()')
    if (guard) return cb(guard)

    if (sbot.db2migrate) {
      onceWhen(
        sbot.db2migrate.synchronized,
        (isSynced) => isSynced === true,
        () => onDrain('keys', next)
      )
    } else {
      onDrain('keys', next)
    }

    function next() {
      indexes['keys'].getSeq(msgId, (err, seq) => {
        // prettier-ignore
        if (err) return cb(new Error('del() failed to find msgId from index', {cause: err}))
        jitdb.lookup('seq', seq, (err, offset) => {
          // prettier-ignore
          if (err) return cb(new Error('del() failed to find seq from jitdb', {cause: err}))
          log.del(offset, cb)
        })
      })
    }
  }

  function deleteFeed(feedId, cb) {
    const guard = guardAgainstDuplicateLogs('deleteFeed()')
    if (guard) return cb(guard)

    onceWhen(
      stateFeedsReady,
      (ready) => ready === true,
      () => {
        pull(
          self.query(
            where(author(feedId)),
            asOffsets(),
            batch(1000),
            toPullStream()
          ),
          pull.asyncMap(log.del),
          pull.onEnd((err) => {
            // prettier-ignore
            if (err) return cb(new Error('deleteFeed() failed for feed ' + feedId, {cause: err}))

            state.delete(feedId)
            indexes.base.removeFeedFromLatest(feedId, cb)
          })
        )
      }
    )
  }

  function stopUpdatingIndexes() {
    if (abortLogStreamForIndexes) {
      abortLogStreamForIndexes()
      abortLogStreamForIndexes = null
    }
  }

  function resumeUpdatingIndexes() {
    if (abortLogStreamForIndexes) return
    else indexesStateLoaded.onReady(updateIndexes)
  }

  function resetAllIndexes(cb) {
    const done = multicb({ pluck: 1 })
    for (const indexName in indexes) {
      indexes[indexName].reset(done())
    }
    done(cb)
  }

  function registerIndex(Index) {
    const index = new Index(log, dir, config.db2)

    if (indexes[index.name]) throw 'Index already exists'

    index.offset((offset) => status.updateIndex(index.name, offset))

    indexes[index.name] = index
  }

  function updateIndexes() {
    if (!log.compactionProgress.value.done) return
    if (abortLogStreamForIndexes) {
      debug('updateIndexes() called while another one is in progress')
      return
    }
    const updatePrivateIndex = true
    const start = Date.now()

    const indexesArr = Object.values(indexes)

    const lowestOffset = Math.min(
      ...indexesArr.map((idx) => idx.offset.value),
      privateIndex.latestOffset.value
    )
    debug(`lowest offset for all indexes is ${lowestOffset}`)

    indexingActive.set(indexingActive.value + 1)
    const sourceOld = log.stream({ gt: lowestOffset, updatePrivateIndex })
    abortLogStreamForIndexes = sourceOld.abort.bind(sourceOld)
    sourceOld.pipe({
      paused: false,
      write(record) {
        const buf = record.value
        const pValue = buf ? bipf.seekKey2(buf, 0, BIPF_VALUE, 0) : -1
        for (const idx of indexesArr) idx.onRecord(record, false, pValue)
      },
      end() {
        debug(`updateIndexes() scan time: ${Date.now() - start}ms`)
        abortLogStreamForIndexes = null
        const doneFlushing = multicb({ pluck: 1 })
        for (const idx of indexesArr) idx.flush(doneFlushing())
        doneFlushing((err) => {
          // prettier-ignore
          if (err) console.error(new Error('updateIndexes() failed to flush indexes', {cause: err}))
          indexingActive.set(indexingActive.value - 1)
          debug('updateIndexes() live streaming')
          const gt = indexes['base'].offset.value
          const sourceLive = log.stream({ gt, live: true, updatePrivateIndex })
          abortLogStreamForIndexes = sourceLive.abort.bind(sourceLive)
          sourceLive.pipe({
            paused: false,
            write(record) {
              const buf = record.value
              const pValue = buf ? bipf.seekKey2(buf, 0, BIPF_VALUE, 0) : -1
              for (const idx of indexesArr) idx.onRecord(record, true, pValue)
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

    secretStackLoaded.onReady(() => {
      if (closed) return
      indexesStateLoaded.onReady(() => {
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
            onceWhen(
              index.offset.bind(index),
              (offset) => !closed && offset === log.since.value,
              () => {
                status.updateIndex(indexName, index.offset.value)
                cb()
              }
            )
          }
        })
      })
    })
  }

  function close(cb) {
    closed = true
    const done = multicb({ pluck: 1 })
    for (const indexName in indexes) {
      const index = indexes[indexName]
      index.close(done())
    }
    privateIndex.close(done())
    done((err) => {
      if (err) return cb(err)
      log.close(cb)
    })
  }

  // override query() from jitdb to implicitly call fromDB()
  function query(first, ...rest) {
    // Before running the query, the log needs to be migrated/synced with the
    // old log and it should be 'drained'
    const waitUntilReady = deferred((meta, cb) => {
      if (sbot.db2migrate) {
        onceWhen(
          sbot.db2migrate.synchronized,
          (isSynced) => isSynced === true,
          next
        )
      } else {
        next()
      }

      function next() {
        onceWhen(
          compacting,
          (isCompacting) => isCompacting === false,
          () => onDrain(cb)
        )
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
      onceWhen(
        sbot.db2migrate.synchronized,
        (isSynced) => isSynced === true,
        next
      )
    } else {
      next()
    }

    function next() {
      jitdb.prepare(operation, cb)
    }
  }

  function reindexOffset(record, seq, pValue, cb) {
    jitdb.reindex(record.offset, (err) => {
      if (err) return cb(new Error('reindexOffset() failed', { cause: err }))

      for (const indexName in indexes) {
        const idx = indexes[indexName]
        if (idx.indexesContent()) idx.processRecord(record, seq, pValue)
      }

      cb()
    })
  }

  function reindexEncrypted(cb) {
    if (!isBrowser) {
      fs.closeSync(fs.openSync(reindexEncryptedInProgressPath(dir), 'w'))
    }
    indexingActive.set(indexingActive.value + 1)
    reindexingLock((unlock) => {
      pull(
        self.query(
          where(isEncrypted('box2')),
          asOffsets(),
          batch(1000),
          toPullStream()
        ),
        pull.asyncMap((offset, cb) => {
          log.get(offset, (err, buf) => {
            // prettier-ignore
            if (err) return cb(new Error('reindexEncrypted() failed when getting messages', {cause: err}))
            const record = { offset, value: buf }

            const pMeta = bipf.seekKey2(buf, 0, BIPF_META, 0)
            if (pMeta < 0) return cb()
            const pPrivate = bipf.seekKey2(buf, pMeta, BIPF_PRIVATE, 0)
            if (pPrivate < 0) return cb()

            // check if we can decrypt the record
            if (!bipf.decode(buf, pPrivate)) return cb()

            const pKey = bipf.seekKey2(buf, 0, BIPF_KEY, 0)
            if (pKey < 0) return cb()
            const key = bipf.decode(buf, pKey)

            const pValue = bipf.seekKey2(buf, 0, BIPF_VALUE, 0)

            reindexedValues(bipf.decode(buf, 0))

            onDrain('keys', () => {
              indexes['keys'].getSeq(key, (err, seq) => {
                // prettier-ignore
                if (err) return cb(new Error('reindexEncrypted() failed when getting seq', {cause: err}))
                reindexOffset(record, seq, pValue, cb)
              })
            })
          })
        }),
        pull.onEnd((err) => {
          if (err) return unlock(cb, err)
          const done = multicb({ pluck: 1 })
          for (const indexName in indexes) {
            const idx = indexes[indexName]
            if (idx.indexesContent()) idx.forcedFlush(done())
          }
          done((err) => {
            // prettier-ignore
            if (err) return unlock(cb, new Error('reindexEncrypted() failed to force-flush indexes', {cause: err}))
            indexingActive.set(indexingActive.value - 1)
            if (!isBrowser) {
              rimraf.sync(reindexEncryptedInProgressPath(dir))
            }
            unlock(cb)
          })
        })
      )
    })
  }

  function notYetZero(obz, fn, ...args) {
    if (obz.value > 0) {
      onceWhen(obz, (x) => x === 0, fn.bind(null, ...args))
      return true
    } else {
      return false
    }
  }

  function compact(cb) {
    if (notYetZero(jitdb.indexingActive, compact, cb)) return
    if (notYetZero(jitdb.queriesActive, compact, cb)) return
    if (notYetZero(indexingActive, compact, cb)) return

    compacting.set(true)
    fs.closeSync(fs.openSync(resetLevelPath(dir), 'w'))
    fs.closeSync(fs.openSync(resetPrivatePath(dir), 'w'))
    log.compact(function onLogCompacted(err) {
      // prettier-ignore
      if (err) cb(new Error('ssb-db2 compact() failed with the log', {cause: err}))
      else cb()
    })
  }

  log.compactionProgress((stats) => {
    compactionProgress(stats)

    if (compacting.value !== !stats.done) compacting.set(!stats.done)

    // fs sync not working in browser
    if (typeof window !== 'undefined') return

    if (stats.done) {
      if (stats.sizeDiff > 0) {
        let resettingLevelIndexes = false
        if (fs.existsSync(resetLevelPath(dir))) {
          resettingLevelIndexes = true
          stopUpdatingIndexes()
          resetAllIndexes(() => {
            rimraf.sync(resetLevelPath(dir))
            resumeUpdatingIndexes()
          })
        }
        if (fs.existsSync(resetPrivatePath(dir))) {
          if (!resettingLevelIndexes) stopUpdatingIndexes()
          privateIndex.reset(() => {
            rimraf.sync(resetPrivatePath(dir))
            if (!resettingLevelIndexes) resumeUpdatingIndexes()
          })
        }
        status.reset()
      } else {
        rimraf.sync(resetLevelPath(dir))
        rimraf.sync(resetPrivatePath(dir))
      }
    }
  })

  function reset(cb) {
    stopUpdatingIndexes()
    const done = multicb({ pluck: 1 })
    status.reset()
    jitdb.reindex(0, done())
    resetAllIndexes(done())
    privateIndex.reset(done())
    done(() => {
      resumeUpdatingIndexes()
      cb()
    })
  }

  function publish() {
    // prettier-ignore
    throw new Error('publish() not installed, you should .use(require("ssb-db2/compat/publish"))')
  }

  function publishAs() {
    // prettier-ignore
    throw new Error('publishAs() not installed, you should .use(require("ssb-db2/compat/publish"))')
  }

  const api = {
    // Public API:
    installFeedFormat,
    installEncryptionFormat,
    get,
    getMsg,
    query,
    prepare,
    del,
    deleteFeed,
    add,
    create,
    addTransaction,
    addOOO,
    addOOOBatch,
    getStatus: () => status.obv,
    getIndexingActive: () => indexingActive,
    operators,
    onMsgAdded,
    compact,
    reindexEncrypted,
    reindexed: () => reindexedValues.listen(),
    logStats: log.stats,
    indexingProgress: () => indexingProgress.listen(),
    compactionProgress: () => compactionProgress.listen(),
    reset,

    // needed primarily internally by other plugins in this project:
    publish,
    publishAs,
    encryptionFormats,
    findFeedFormatByName,
    findFeedFormatForAuthor,
    findEncryptionFormatFor,
    findEncryptionFormatByName,
    addBatch,
    addImmediately,
    getLatest: indexes.base.getLatest.bind(indexes.base),
    getAllLatest: indexes.base.getAllLatest.bind(indexes.base),
    getEncryptedOffsets: privateIndex.getEncryptedOffsets,
    getDecryptedOffsets: privateIndex.getDecryptedOffsets,
    getLog: () => log,
    registerIndex,
    setStateFeedsReady,
    loadStateFeeds,
    stateFeedsReady,
    getState: () => state,
    getIndexes: () => indexes,
    getIndex: (index) => indexes[index],
    onDrain,
    getJITDB: () => jitdb,
  }

  // Copy api to self
  for (const key in api) {
    self[key] = api[key]
  }

  return self
}
