// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const os = require('os')
const fs = require('fs')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const clarify = require('clarify-error')
const push = require('push-stream')
const Notify = require('pull-notify')
const bipf = require('bipf')
const pull = require('pull-stream')
const paramap = require('pull-paramap')
const Obv = require('obz')
const jitdbOperators = require('jitdb/operators')
const JITDB = require('jitdb')
const Debug = require('debug')
const multicb = require('multicb')
const mutexify = require('mutexify')

const operators = require('./operators')
const {
  jitIndexesPath,
  resetLevelPath,
  resetPrivatePath,
  reindexJitPath,
} = require('./defaults')
const { onceWhen, ReadyGate } = require('./utils')
const DebouncingBatchAdd = require('./debounce-batch')
const Log = require('./log')
const Status = require('./status')
const makeBaseIndex = require('./indexes/base')
const KeysIndex = require('./indexes/keys')
const PrivateIndex = require('./indexes/private')

const BIPF_VALUE = bipf.allocAndEncode('value')
const BIPF_KEY = bipf.allocAndEncode('key')
const BIPF_META = bipf.allocAndEncode('meta')
const BIPF_PRIVATE = bipf.allocAndEncode('private')

const { where, fromDB, author, deferred, asOffsets, toCallback } = operators

exports.name = 'db'

exports.version = '1.9.1'

exports.manifest = {
  get: 'async',
  add: 'async',
  create: 'async',
  publish: 'async', // FIXME: move out of here?
  publishAs: 'async', // FIXME: move out of here?
  del: 'async',
  deleteFeed: 'async',
  addTransaction: 'async',
  addOOO: 'async',
  addBatch: 'async',
  addOOOBatch: 'async',
  getStatus: 'sync',
  compact: 'async',
  indexingProgress: 'source',

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
  const post = Obv() // FIXME: move to compat?
  const onMsgAdded = Obv()
  const indexingProgress = Notify()
  const indexingActive = Obv().set(0)
  let abortLogStreamForIndexes = null
  const compacting = Obv().set(false)
  const hmacKey = null
  const stateFeedsReady = Obv().set(false)
  const secretStackLoaded = new ReadyGate()
  const indexesStateLoaded = new ReadyGate()

  sbot.close.hook(function (fn, args) {
    close((err) => {
      fn.apply(this, args)
    })
  })

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
      indexesStateLoaded.setReady()
    })
  })

  // FIXME: we need to wait for box1 and box2 async "ready" to load

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
        throw new Error('No feed format installed understands ' + feedId)
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
    getAsKV(feedId) {
      const nativeMsg = this._map.get(feedId)
      if (!nativeMsg) return null
      const feedFormat = findFeedFormatForAuthor(feedId)
      if (!feedFormat) {
        throw new Error('No feed format installed understands ' + feedId)
      }
      const key = feedFormat.getMsgId(nativeMsg, 'js')
      const value = feedFormat.fromNativeMsg(nativeMsg, 'js')
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
        pull.collect((err, kvts) => {
          if (err) return console.error(clarify(err, 'loadStateFeeds failed'))
          for (const kvt of kvts) {
            state.updateFromKVT(PrivateIndex.reEncrypt(kvt))
          }
          debug('getAllLatest is done setting up initial validate state')
          if (!stateFeedsReady.value) stateFeedsReady.set(true)
          if (cb) cb()
        })
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
        if (err) cb(clarify(err, 'Msg ' + id + ' not found in leveldb index'))
        else {
          jitdb.lookup('seq', seq, (err, offset) => {
            if (err) cb(clarify(err, 'Msg ' + id + ' not found in jit index'))
            else {
              getMsgByOffset(offset, (err, msg) => {
                if (err) cb(clarify(err, 'Msg ' + id + ' not found in the log'))
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

  function addFeedFormat(feedFormat) {
    if (!feedFormat.encodings.includes('js')) {
      // prettier-ignore
      throw new Error('ssb-db2: feed format ' + feedFormat.name + ' must have js encoding')
    }
    feedFormats.push(feedFormat)
  }

  function addEncryptionFormat(encryptionFormat) {
    encryptionFormats.push(encryptionFormat)
  }

  function findEncryptionFormatFor(ciphertextJS) {
    if (!ciphertextJS) return null
    if (typeof ciphertextJS !== 'string') return null
    for (const encryptionFormat of encryptionFormats) {
      if (ciphertextJS.endsWith(`.${encryptionFormat.suffix}`)) {
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

  const debouncePeriod = config.db2.addDebounce || 250
  const debouncer = new DebouncingBatchAdd(addBatch, debouncePeriod)

  function normalizeAddArgs(...args) {
    let cb, opts
    if (typeof args[0] === 'function') {
      opts = { encoding: 'js' }
      cb = args[0]
    } else if (!args[0] && typeof args[1] === 'function') {
      opts = { encoding: 'js' }
      cb = args[1]
    } else if (typeof args[0] === 'object' && typeof args[1] === 'function') {
      opts = args[0]
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
        if (feedFormat.validateBatch) debouncer.add(nativeMsg, opts, cb)
        else addImmediately(nativeMsg, opts, cb)
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

        feedFormat.validateBatch(hmacKey, nativeMsgs, prevNativeMsg, (err) => {
          if (err) return cb(clarify(err, 'validation in addBatch() failed'))

          const done = multicb({ pluck: 1 })
          for (var i = 0; i < nativeMsgs.length; ++i) {
            const nativeMsg = nativeMsgs[i]
            const msgId = feedFormat.getMsgId(nativeMsg)
            const msgVal = feedFormat.fromNativeMsg(nativeMsg, opts.encoding)
            const isLast = i === nativeMsgs.length - 1

            if (isLast) {
              state.update(feedId, nativeMsg)
            }

            log.add(msgId, msgVal, feedId, opts.encoding, (err, kvt) => {
              // prettier-ignore
              if (err) return done()(clarify(err, 'addBatch() failed in the log'))

              post.set(kvt)
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

  function addImmediately(nativeMsg, ...args) {
    const guard = guardAgainstDuplicateLogs('addImmediately()')
    if (guard) return cb(guard)
    const [opts, cb] = normalizeAddArgs(...args)
    const feedFormat = findFeedFormatByNameOrNativeMsg(
      opts.feedFormat,
      nativeMsg
    )
    if (!feedFormat) {
      // prettier-ignore
      return cb(new Error('addImmediately() failed because could not find feed format for: ' + nativeMsg))
    }

    onceWhen(
      stateFeedsReady,
      (ready) => ready === true,
      () => {
        const feedId = feedFormat.getFeedId(nativeMsg)
        const prevNativeMsg = state.get(feedId)
        feedFormat.validateSingle(hmacKey, nativeMsg, prevNativeMsg, (err) => {
          // prettier-ignore
          if (err) return cb(clarify(err, 'addImmediately() failed validation for feed format ' + feedFormat.name))
          const msgId = feedFormat.getMsgId(nativeMsg)
          const msgVal = feedFormat.fromNativeMsg(nativeMsg, opts.encoding)
          state.update(feedId, nativeMsg)
          log.add(msgId, msgVal, feedId, opts.encoding, (err, kvt) => {
            // prettier-ignore
            if (err) return cb(clarify(err, 'addImmediately() failed in the log'))

            post.set(kvt)
            onMsgAdded.set({
              kvt,
              nativeMsg,
              feedFormat: feedFormat.name,
            })
            cb(null, kvt)
          })
        })
      }
    )
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
    if (!feedFormat.validateOOOBatch) {
      // prettier-ignore
      return cb(new Error('addOOO() failed because feed format ' + feedFormat.name + ' does not support validateOOOBatch'))
    }

    feedFormat.validateOOOBatch(hmacKey, [nativeMsg], (err) => {
      // prettier-ignore
      if (err) return cb(clarify(err, 'addOOO() failed validation for feed format ' + feedFormat.name))
      const msgId = feedFormat.getMsgId(nativeMsg)
      get(msgId, (err, data) => {
        if (data) return cb(null, data)
        const msgVal = feedFormat.fromNativeMsg(nativeMsg, opts.encoding)
        const feedId = feedFormat.getFeedId(nativeMsg)
        log.add(msgId, msgVal, feedId, opts.encoding, (err, data) => {
          if (err) return cb(clarify(err, 'addOOO() failed in the log'))
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

    onceWhen(
      stateFeedsReady,
      (ready) => ready === true,
      () => {
        feedFormat.validateOOOBatch(hmacKey, nativeMsgs, (err) => {
          if (err) return cb(clarify(err, 'validation in addOOOBatch() failed'))

          const done = multicb({ pluck: 1 })
          for (var i = 0; i < nativeMsgs.length; ++i) {
            const msgId = feedFormat.getMsgId(nativeMsgs[i])
            const msgVal = feedFormat.fromNativeMsg(
              nativeMsgs[i],
              opts.encoding
            )
            const feedId = feedFormat.getFeedId(nativeMsgs[i])
            log.add(msgId, msgVal, feedId, opts.encoding, done())
          }

          done(cb)
        })
      }
    )
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
    const exampleNativeMsg =
      nativeMsgs.length > 0 ? nativeMsgs[0] : oooNativeMsgs[0]
    const feedFormat = findFeedFormatByNameOrNativeMsg(
      opts.feedFormat,
      exampleNativeMsg
    )
    if (!feedFormat) {
      // prettier-ignore
      return cb(new Error('addTransaction() failed because could not find feed format for: ' + exampleNativeMsg))
    }
    if (!feedFormat.validateBatch) {
      // prettier-ignore
      return cb(new Error('addTransaction() failed because feed format ' + feedFormat.name + ' does not support validateBatch'))
    }
    if (!feedFormat.validateOOOBatch) {
      // prettier-ignore
      return cb(new Error('addTransaction() failed because feed format ' + feedFormat.name + ' does not support validateOOOBatch'))
    }

    onceWhen(
      stateFeedsReady,
      (ready) => ready === true,
      () => {
        const done = multicb({ pluck: 1 })

        if (nativeMsgs.length > 0) {
          const feedId = feedFormat.getFeedId(nativeMsgs[0])
          const prevNativeMsg = state.get(feedId)
          feedFormat.validateBatch(hmacKey, nativeMsgs, prevNativeMsg, done())
        } else {
          done()(null, [])
        }

        feedFormat.validateOOOBatch(hmacKey, oooNativeMsgs, done())

        done((err) => {
          // prettier-ignore
          if (err) return cb(clarify(err, 'validation in addTransaction() failed'))

          const msgIds = nativeMsgs.map((nMsg) => feedFormat.getMsgId(nMsg))
          const oooMsgIds = oooNativeMsgs.map((nMsg) =>
            feedFormat.getMsgId(nMsg)
          )

          if (nativeMsgs.length > 0) {
            const lastIndex = nativeMsgs.length - 1
            const nativeMsg = nativeMsgs[lastIndex]
            const feedId = feedFormat.getFeedId(nativeMsg)
            state.update(feedId, nativeMsg)
          }

          const allMsgIds = [].concat(msgIds, oooMsgIds)
          const allMsgVals = []
            .concat(nativeMsgs, oooNativeMsgs)
            .map((nMsg) => feedFormat.fromNativeMsg(nMsg, opts.encoding))

          log.addTransaction(
            allMsgIds,
            allMsgVals,
            opts.encoding,
            (err, kvts) => {
              if (err)
                return cb(clarify(err, 'addTransaction() failed in the log'))
              if (kvts.length !== allMsgIds.length) {
                // prettier-ignore
                return cb(new Error('addTransaction() failed due to mismatched message count'))
              }

              for (let i = 0; i < kvts.length; ++i) {
                post.set(kvts[i])
                onMsgAdded.set({
                  kvt: kvts[i],
                  nativeMsg: allMsgVals[i],
                  feedFormat: feedFormat.name,
                })
              }
              cb(null, kvts)
            }
          )
        })
      }
    )
  }

  function create(opts, cb) {
    const guard = guardAgainstDuplicateLogs('create()')
    if (guard) return cb(guard)

    const keys = opts.keys || config.keys

    const feedFormat = findFeedFormatByName(opts.feedFormat || 'classic')
    const encoding = opts.encoding || 'js'
    // prettier-ignore
    if (!feedFormat) return cb(new Error('create() does not support feed format ' + opts.feedFormat))
    if (!feedFormat.isAuthor(keys.id)) {
      // prettier-ignore
      return cb(new Error(`create() failed because keys.id ${keys.id} is not a valid author for feed format ${feedFormat.name}`))
    }

    if (!opts.content) return cb(new Error('create() requires a `content`'))

    onceWhen(
      stateFeedsReady,
      (ready) => ready === true,
      () => {
        const provisionalNativeMsg = feedFormat.newNativeMsg({
          timestamp: Date.now(),
          ...opts,
          previous: null,
          keys,
        })
        const feedId = feedFormat.getFeedId(provisionalNativeMsg)
        const previous = state.getAsKV(feedId)
        const fullOpts = { timestamp: Date.now(), ...opts, previous, keys }

        // If the inputs require encryption, try some encryption formats,
        // and pick the best output.
        if (opts.recps || opts.content.recps) {
          const plaintext = feedFormat.toPlaintextBuffer(fullOpts)
          function encryptWith(encryptionFormat) {
            const recipients = encryptionFormat.getRecipients(fullOpts)
            const ciphertextBuf = encryptionFormat.encrypt(
              plaintext,
              recipients,
              fullOpts
            )
            return (
              ciphertextBuf.toString('base64') + '.' + encryptionFormat.suffix
            )
          }
          if (opts.encryptionFormat) {
            const format = findEncryptionFormatByName(opts.encryptionFormat)
            const ciphertext = encryptWith(format)
            fullOpts.content = ciphertext
          } else {
            const outputs = encryptionFormats.map((format) => {
              try {
                const ciphertext = encryptWith(format)
                return [null, { ciphertext, name: format.name }]
              } catch (err) {
                return [err]
              }
            })
            const successes = outputs
              .filter(([err]) => !err)
              .map(([, success]) => success)
            const errors = outputs
              .filter(([err]) => err)
              .map(([error]) => error.message)
            if (successes.length === 0) {
              // prettier-ignore
              return cb(new Error('create() failed to encrypt content: ' + errors.join('; ')))
            }
            fullOpts.content = successes[0].ciphertext
          }
        }

        fullOpts.hmacKey = hmacKey
        const nativeMsg = feedFormat.newNativeMsg(fullOpts)
        const msgId = feedFormat.getMsgId(nativeMsg)

        const encodedMsg = feedFormat.fromNativeMsg(nativeMsg, encoding)
        state.update(feedId, nativeMsg)
        log.add(msgId, encodedMsg, feedId, encoding, (err, encodedKVT) => {
          if (err) return cb(clarify(err, 'create() failed in the log'))

          post.set(encodedKVT)
          onMsgAdded.set({
            kvt: encodedKVT,
            nativeMsg: nativeMsg,
            feedFormat: feedFormat.name,
          })
          cb(null, encodedKVT)
        })
      }
    )
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
        if (err) return cb(clarify(err, 'del() failed to find msgId from index'))
        jitdb.lookup('seq', seq, (err, offset) => {
          // prettier-ignore
          if (err) return cb(clarify(err, 'del() failed to find seq from jitdb'))
          log.del(offset, cb)
        })
      })
    }
  }

  function deleteFeed(feedId, cb) {
    const guard = guardAgainstDuplicateLogs('deleteFeed()')
    if (guard) return cb(guard)

    self.query(
      where(author(feedId)),
      asOffsets(),
      toCallback((err, offsets) => {
        if (err) return cb(clarify(err, 'deleteFeed() failed to query jitdb'))
        push(
          push.values(offsets),
          push.asyncMap(log.del),
          push.collect((err) => {
            if (err) cb(clarify(err, 'deleteFeed() failed for feed ' + feedId))
            else {
              state.delete(feedId)
              indexes.base.removeFeedFromLatest(feedId, cb)
            }
          })
        )
      })
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
    const index = new Index(log, dir)

    if (indexes[index.name]) throw 'Index already exists'

    index.offset((o) => status.updateIndex(index.name, o))

    indexes[index.name] = index
  }

  function updateIndexes() {
    if (!log.compactionProgress.value.done) return
    if (abortLogStreamForIndexes) {
      debug('updateIndexes() called while another one is in progress')
      return
    }
    const start = Date.now()

    const indexesArr = Object.values(indexes)

    const lowestOffset = Math.min(
      ...indexesArr.map((idx) => idx.offset.value),
      privateIndex.latestOffset.value
    )
    debug(`lowest offset for all indexes is ${lowestOffset}`)

    indexingActive.set(indexingActive.value + 1)
    const sink = log.stream({ gt: lowestOffset }).pipe({
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
          if (err) console.error(clarify(err, 'updateIndexes() failed to flush indexes'))
          indexingActive.set(indexingActive.value - 1)
          debug('updateIndexes() live streaming')
          const gt = indexes['base'].offset.value
          const sink = log.stream({ gt, live: true }).pipe({
            paused: false,
            write(record) {
              const buf = record.value
              const pValue = buf ? bipf.seekKey2(buf, 0, BIPF_VALUE, 0) : -1
              for (const idx of indexesArr) idx.onRecord(record, true, pValue)
            },
          })
          abortLogStreamForIndexes = sink.source.abort.bind(sink.source)
        })
      },
    })
    abortLogStreamForIndexes = sink.source.abort.bind(sink.source)
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
      if (err) return cb(clarify(err, 'reindexOffset() failed'))

      for (const indexName in indexes) {
        const idx = indexes[indexName]
        if (idx.indexesContent()) idx.processRecord(record, seq, pValue)
      }

      cb()
    })
  }

  const reindexingLock = mutexify()

  function reindexEncrypted(cb) {
    indexingActive.set(indexingActive.value + 1)
    reindexingLock((unlock) => {
      const offsets = privateIndex.getUnsolved('box2')
      const keysIndex = indexes['keys']

      push(
        push.values(offsets),
        push.asyncMap((offset, cb) => {
          log.get(offset, (err, buf) => {
            // prettier-ignore
            if (err) return cb(clarify(err, 'reindexEncrypted() failed when getting messages'))
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

            onDrain('keys', () => {
              keysIndex.getSeq(key, (err, seq) => {
                // prettier-ignore
                if (err) return cb(clarify(err, 'reindexEncrypted() failed when getting seq'))
                reindexOffset(record, seq, pValue, cb)
              })
            })
          })
        }),
        push.collect((err) => {
          if (err) return unlock(cb, err)
          const done = multicb({ pluck: 1 })
          for (const indexName in indexes) {
            const idx = indexes[indexName]
            if (idx.indexesContent()) idx.forcedFlush(done())
          }
          done((err) => {
            // prettier-ignore
            if (err) return unlock(cb, clarify(err, 'reindexEncrypted() failed to force-flush indexes'))
            indexingActive.set(indexingActive.value - 1)
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

    fs.closeSync(fs.openSync(resetLevelPath(dir), 'w'))
    fs.closeSync(fs.openSync(resetPrivatePath(dir), 'w'))
    fs.closeSync(fs.openSync(reindexJitPath(dir), 'w'))
    log.compact(function onLogCompacted(err) {
      if (err) cb(clarify(err, 'ssb-db2 compact() failed with the log'))
      else cb()
    })
  }

  let compactStartOffset = null
  log.compactionProgress((stats) => {
    if (typeof stats.startOffset === 'number' && compactStartOffset === null) {
      compactStartOffset = stats.startOffset
    }

    if (compacting.value !== !stats.done) compacting.set(!stats.done)

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
        if (fs.existsSync(reindexJitPath(dir))) {
          jitdb.reindex(compactStartOffset || 0, (err) => {
            if (err) console.error('ssb-db2 reindex jitdb after compact', err)
            rimraf.sync(reindexJitPath(dir))
          })
        }
        status.reset()
        compactStartOffset = null
      } else {
        rimraf.sync(resetLevelPath(dir))
        rimraf.sync(resetPrivatePath(dir))
        rimraf.sync(reindexJitPath(dir))
      }
    }
  })

  const api = {
    // Public API:
    addFeedFormat,
    addEncryptionFormat,
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
    operators,
    onMsgAdded,
    post,
    compact,
    reindexEncrypted,
    indexingProgress: () => indexingProgress.listen(),

    // used for partial replication in browser, will be removed soon!
    setPost: post.set,

    // needed primarily internally by other plugins in this project:
    encryptionFormats,
    findFeedFormatByName,
    findFeedFormatForAuthor,
    findEncryptionFormatFor,
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
    onDrain,
    getJITDB: () => jitdb,
  }

  // Copy api to self
  for (const key in api) {
    self[key] = api[key]
  }

  return self
}
