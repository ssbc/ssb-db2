// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const fs = require('fs')
const pull = require('pull-stream')
const Notify = require('pull-notify')
const drainGently = require('pull-drain-gently')
const clarify = require('clarify-error')
const FlumeLog = require('flumelog-offset')
const AsyncLog = require('async-append-only-log')
const bipf = require('bipf')
const Obv = require('obz')
const rimraf = require('rimraf')
const jsonCodec = require('flumecodec/json')
const debug = require('debug')('ssb:db2:migrate')
const {
  BLOCK_SIZE,
  flumePath,
  oldLogPath,
  newLogPath,
  tooHotOpts,
} = require('./defaults')
const seekers = require('./seekers')

function fileExists(filename) {
  return fs.existsSync(filename) && fs.statSync(filename).size > 0
}

// Forked from flumecodec because we have to support
// bendy butt messages which may contain Buffers
const jsonCodecForSSBFixtures = {
  encode: JSON.stringify,
  decode(str) {
    const parsed = JSON.parse(str)
    const content = parsed.value.content
    if (content.type === 'metafeed/add/derived') {
      for (const key of Object.keys(content)) {
        const field = content[key]
        if (field.type === 'Buffer' && Array.isArray(field.data)) {
          content[key] = Buffer.from(field.data)
        }
      }
    }
    return parsed
  },
  buffer: false,
  type: 'json',
}

function getOldLog(sbot, config) {
  const oldLog = FlumeLog(oldLogPath(config.path), {
    blockSize: BLOCK_SIZE,
    codec: config.db2._ssbFixtures ? jsonCodecForSSBFixtures : jsonCodec,
  })
  const opts = {
    keys: true,
    seqs: true,
    value: true,
    sync: false,
    reverse: false,
    codec: config.db2._ssbFixtures ? jsonCodecForSSBFixtures : jsonCodec,
  }
  const getStream = (moreOpts) =>
    oldLog.stream({ old: true, live: false, ...opts, ...moreOpts })
  const getLiveStream = () => oldLog.stream({ old: false, live: true, ...opts })
  const getSize = () => oldLog.since.value
  // FIXME: when we do #129, should replace Obv() with something from db.js
  const newMsgObv = sbot.post ? sbot.post : Obv()
  return { getStream, getLiveStream, getSize, newMsgObv }
}

function toBIPF(msg) {
  const len = bipf.encodingLength(msg)
  const buf = Buffer.alloc(len)
  bipf.encode(msg, buf, 0)
  return buf
}

function scanAndCount(pushstream, cb) {
  let count = 0
  pushstream.pipe({
    paused: false,
    write() {
      count += 1
    },
    end(err) {
      // prettier-ignore
      if (err) cb(clarify(err, 'scanAndCount() failed scanning async-append-only-log'))
      else cb(null, count)
    },
  })
}

function guardAgainstDecryptedMsg(msg) {
  if (
    (msg && msg.meta && msg.meta.private) ||
    (msg && msg.value && msg.value.meta && msg.value.meta.private)
  ) {
    return new Error(
      'ssb:db2:migrate was about to write ' +
        'private message *decrypted* to disk'
    )
  }
}

/**
 * Fallback algorithm that does the same as findMigratedOffset, but is slow
 * because it does a scan of BOTH new log and old log. First, it scans the new
 * log to count how many msgs there exists, then it scans the old log to match
 * that count.
 */
function inefficientFindMigratedOffset(newLog, oldLog, cb) {
  scanAndCount(newLog.stream({ gte: 0, decrypt: false }), (err, msgCount) => {
    if (err) return cb(err)
    if (!msgCount) return cb(null, -1)

    let result = -1
    pull(
      oldLog.getStream({ gte: 0 }),
      pull.take(msgCount),
      pull.drain(
        (x) => {
          result = x.seq
        },
        (err) => {
          // prettier-ignore
          if (err) cb(clarify(err, 'inefficientFindMigratedOffset() failed scanning flumelog'))
          else cb(null, result)
        }
      )
    )
  })
}

function findMigratedOffset(sbot, oldLog, newLog, cb) {
  if (!sbot.get) {
    debug('running in inefficient mode because no ssb-db is installed')
    inefficientFindMigratedOffset(newLog, oldLog, cb)
    return
  }

  newLog.onDrain(() => {
    if (typeof newLog.since.value !== 'number' || newLog.since.value < 0) {
      cb(null, -1)
      return
    }

    const offsetInNewLog = newLog.since.value
    newLog.get(offsetInNewLog, (err, buf) => {
      // prettier-ignore
      if (err) return cb(clarify(err, 'findMigratedOffset() failed to get msg in async-append-only-log'))

      const msgKey = bipf.decode(buf, seekers.seekKey(buf))
      sbot.get(msgKey, (err2, msg, offsetInOldLog) => {
        // prettier-ignore
        if (err2) return cb(clarify(err2, 'findMigratedOffset() failed to get msg in flumelog'))

        if (typeof offsetInOldLog === 'number') {
          cb(null, offsetInOldLog)
        } else {
          // NOTE! Currently all versions of ssb-db do not support returning
          // byte offset, so this case will trigger always! The only way to
          // make ssb-db support it is to hack it with patch-package. This is
          // fine temporarily because the only change is faster performance.
          debug(
            'running in inefficient mode because your ssb-db ' +
              'does not support returning byte offset from ssb.get()'
          )
          inefficientFindMigratedOffset(newLog, oldLog, cb)
        }
      })
    })
  })
}

exports.name = 'db2migrate'

exports.version = '1.9.1'

exports.manifest = {
  start: 'sync',
  stop: 'sync',
  doesOldLogExist: 'sync',
  synchronized: 'async',
  progress: 'source',
}

exports.init = function init(sbot, config) {
  config = config || {}
  config.db2 = config.db2 || {}
  const oldLogExists = Obv().set(fileExists(oldLogPath(config.path)))

  /**
   * Boolean obv that indicates whether the new log is synced with the old log.
   */
  const synchronized = Obv()
  synchronized.set(true) // assume true until we `start()`

  const progressStream = Notify()
  let started = false
  let hasCloseHook = false
  let retryPeriod = 250
  let drainAborter = null
  let liveProgressInterval = null

  function oldLogMissingThenRetry(retryFn) {
    if (!hasCloseHook) {
      sbot.close.hook(function (fn, args) {
        stop()
        fn.apply(this, args)
      })
      hasCloseHook = true
    }
    oldLogExists.set(fileExists(oldLogPath(config.path)))
    if (oldLogExists.value === false) {
      retryPeriod = Math.min(retryPeriod * 2, 8000)
      setTimeout(retryFn, retryPeriod).unref()
      return true
    } else {
      return false
    }
  }

  function guardAgainstMigrationDangers() {
    if (sbot.messagesByType && config.db2.dangerouslyKillFlumeWhenMigrated) {
      return new Error(
        'we cannot have ssb-db installed simultaneously with ' +
          'config.db2.dangerouslyKillFlumeWhenMigrated enabled'
      )
    }
  }

  if (config.db2 && config.db2.automigrate) {
    start()
  }

  function stop() {
    started = false
    if (drainAborter) {
      drainAborter.abort()
      drainAborter = null
    }
    if (liveProgressInterval) {
      clearInterval(liveProgressInterval)
      liveProgressInterval = null
    }
  }

  function start() {
    if (started) return
    const guard = guardAgainstMigrationDangers()
    if (guard) throw guard
    if (oldLogMissingThenRetry(start)) return
    started = true
    debug('started')

    if (sbot.db) sbot.db.setStateFeedsReady('migrating')
    synchronized.set(false)

    const oldLog = getOldLog(sbot, config)
    const newLog =
      sbot.db && sbot.db.getLog() && sbot.db.getLog().stream
        ? sbot.db.getLog()
        : AsyncLog(newLogPath(config.path), { blockSize: BLOCK_SIZE })

    let migratedSize = 0

    function updateMigratedSizeAndPluck(obj) {
      // "seq" in flumedb is an abstract num, here it actually means "offset"
      migratedSize = obj.seq
      return obj.value
    }

    let previousProgress = -1
    function emitProgressEvent() {
      const oldSize = oldLog.getSize()
      if (migratedSize === 0 && oldSize === 0) {
        progressStream(1)
        return
      }
      if (!oldSize) return // avoid division by zero
      const progress = Math.min(migratedSize / oldSize, 1)
      if (progress === 1 || progress !== previousProgress) {
        progressStream(progress)
        previousProgress = progress
      }
    }

    let dataTransferred = 0 // FIXME: does this only work if the new log is empty?
    function writeToNewLog(data, cb) {
      dataTransferred += data.length
      // FIXME: could we use log.add since it already converts to BIPF?
      // FIXME: see also issue #16
      newLog.append(data, () => {})
      if (dataTransferred % BLOCK_SIZE === 0) newLog.onDrain(cb)
      else cb()
    }

    function migrateLive() {
      let liveMsgCount = 0
      // Setup periodic progress event and `debug` reporter of live migrate
      liveProgressInterval = setInterval(() => {
        emitProgressEvent()
        if (liveMsgCount > 0) {
          debug('%d msgs synced from old log to new log', liveMsgCount)
          liveMsgCount = 0
        }
      }, 3000).unref()

      // Setup migration of live new msgs identified on the old log
      oldLog.newMsgObv((msg) => {
        const guard = guardAgainstDecryptedMsg(msg)
        if (guard) throw guard

        writeToNewLog(toBIPF(msg), () => {
          liveMsgCount++
        })
      })
    }

    findMigratedOffset(sbot, oldLog, newLog, (err, migratedOffset) => {
      if (err) return console.error(err)

      if (migratedOffset >= 0) {
        debug('continue migrating from previous offset %s', migratedOffset)
        migratedSize = migratedOffset
      }
      emitProgressEvent()

      let msgCountOldLog = 0
      function op() {
        msgCountOldLog++
      }

      const progressInterval = setInterval(emitProgressEvent, 3000)
      function opDone(err) {
        if (err) return console.error(err)

        // Inform the other parts of ssb-db2 that migration is done
        function doneMigrating() {
          clearInterval(progressInterval)
          emitProgressEvent()
          synchronized.set(true)
          debug('done migrating %s msgs from old log', msgCountOldLog)
          drainAborter = null
        }

        if (config.db2.dangerouslyKillFlumeWhenMigrated) {
          rimraf(flumePath(config.path), (err) => {
            if (err) return console.error(err)
            if (sbot.db) {
              sbot.db.loadStateFeeds(() => {
                sbot.db.setStateFeedsReady(true)
                oldLogExists.set(false)
                doneMigrating()
              })
            } else {
              oldLogExists.set(false)
              doneMigrating()
            }
          })
        } else {
          if (sbot.db) {
            sbot.db.loadStateFeeds(() => {
              sbot.db.setStateFeedsReady(true)
            })
          }
          doneMigrating()
          migrateLive()
        }
      }

      pull(
        oldLog.getStream({ gt: migratedOffset }),
        pull.map(updateMigratedSizeAndPluck),
        pull.map(toBIPF),
        pull.asyncMap(writeToNewLog),
        (drainAborter = config.db2.maxCpu
          ? drainGently(tooHotOpts(config), op, opDone)
          : pull.drain(op, opDone))
      )
    })
  }

  return {
    start,
    stop,
    doesOldLogExist: () => oldLogExists.value,
    synchronized,
    progress: () => progressStream.listen(),
  }
}
