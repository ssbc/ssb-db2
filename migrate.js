const fs = require('fs')
const pull = require('pull-stream')
const drainGently = require('pull-drain-gently')
const Notify = require('pull-notify')
const FlumeLog = require('flumelog-offset')
const AsyncFlumeLog = require('async-flumelog')
const bipf = require('bipf')
const jsonCodec = require('flumecodec/json')
const Obv = require('obv')
const debug = require('debug')('ssb:db2:migrate')
const { BLOCK_SIZE, oldLogPath, newLogPath } = require('./defaults')

function skip(count, onDone) {
  let skipped = 0
  return pull.filter((x) => {
    if (skipped >= count) return true
    else {
      skipped++
      if (skipped === count && onDone) onDone(x)
      return false
    }
  })
}

function fileExists(filename) {
  return fs.existsSync(filename) && fs.statSync(filename).size > 0
}

function makeFileExistsObv(filename) {
  const obv = Obv()
  obv.set(fileExists(filename))
  return obv
}

function getOldLogStreams(sbot, config) {
  if (sbot.createRawLogStream && sbot.createSequenceStream) {
    const logStream = sbot.createRawLogStream({ old: true, live: false })
    const logStreamLive = sbot.createRawLogStream({ old: false, live: true })
    const sizeStream = pull(
      sbot.createSequenceStream(),
      pull.filter((x) => x >= 0)
    )
    return [logStream, logStreamLive, sizeStream]
  } else {
    const oldLog = FlumeLog(oldLogPath(config.path), {
      blockSize: BLOCK_SIZE,
      codec: jsonCodec,
    })
    const opts = { seqs: true, codec: jsonCodec }
    const logStream = oldLog.stream({ old: true, live: false, ...opts })
    const logStreamLive = oldLog.stream({ old: false, live: true, ...opts })
    const notify = Notify()
    oldLog.since(notify)
    const sizeStream = pull(
      notify.listen(),
      pull.filter((x) => x >= 0)
    )
    return [logStream, logStreamLive, sizeStream]
  }
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
    write: () => {
      count += 1
    },
    end: function (err) {
      if (this.ended) return
      this.ended = err || true
      cb(null, count)
    },
  })
}

exports.name = 'db2migrate'

exports.init = function init(sbot, config, newLogMaybe) {
  const oldLogExists = makeFileExistsObv(oldLogPath(config.path))
  const maxCpu =
    config.db2 && typeof config.db2.migrateMaxCpu === 'number'
      ? config.db2.migrateMaxCpu
      : Infinity
  const maxPause =
    config.db2 && typeof config.db2.migrateMaxPause === 'number'
      ? config.db2.migrateMaxPause
      : 10e3 // seconds

  let started = false
  let hasCloseHook = false
  let retryPeriod = 250
  let timer

  function oldLogMissingRetry(fn) {
    if (!hasCloseHook) {
      sbot.close.hook(function (fn, args) {
        clearTimeout(timer)
        fn.apply(this, args)
      })
      hasCloseHook = true
    }
    oldLogExists.set(fileExists(oldLogPath(config.path)))
    if (oldLogExists.value === false) {
      timer = setTimeout(fn, (retryPeriod = Math.min(retryPeriod * 2, 8000)))
      return true
    } else {
      return false
    }
  }

  if (config.db2 && config.db2.automigrate) {
    start()
  }

  function start() {
    if (started) return
    if (oldLogMissingRetry(start)) return
    started = true
    debug('started')

    const [oldLogStream, oldLogStreamLive, oldSizeStream] = getOldLogStreams(
      sbot,
      config
    )
    const newLog =
      newLogMaybe && newLogMaybe.stream
        ? newLogMaybe
        : AsyncFlumeLog(newLogPath(config.path), { blockSize: BLOCK_SIZE })
    const newLogStream = newLog.stream({ gte: 0 })

    let oldSize = null
    let migratedSize = null
    let progressCalls = 0

    function updateOldSize(read) {
      read(null, function next(end, data) {
        if (end === true) return
        if (end) throw end
        oldSize = data
        read(null, next)
      })
    }

    function updateMigratedSize(obj) {
      migratedSize = obj.seq
    }

    function updateMigratedSizeAndPluck(obj) {
      updateMigratedSize(obj)
      return obj.value
    }

    function emitProgressEvent() {
      if (oldSize !== null && migratedSize !== null) {
        if (progressCalls < 100 || progressCalls++ % 1000 == 0) {
          const progress = migratedSize / oldSize
          sbot.emit('ssb:db2:migrate:progress', progress)
        }
      }
    }

    let dataTransferred = 0 // FIXME: does this only work if the new log is empty?
    function writeTo(log) {
      return (data, cb) => {
        dataTransferred += data.length
        // FIXME: could we use log.add since it already converts to BIPF?
        // FIXME: see also issue #16
        log.append(data, () => {})
        emitProgressEvent()
        if (dataTransferred % BLOCK_SIZE === 0) log.onDrain(cb)
        else cb()
      }
    }

    function drainMaybeGently(op, cb) {
      if (isFinite(maxCpu)) {
        debug('reading old log only when CPU is less than ' + maxCpu + '% busy')
        return drainGently({ ceiling: maxCpu, wait: 60, maxPause }, op, cb)
      } else {
        debug('reading old log')
        return pull.drain(op, cb)
      }
    }

    updateOldSize(oldSizeStream)

    scanAndCount(newLogStream, (err, msgCountNewLog) => {
      if (err) return console.error(err)
      if (msgCountNewLog === 0) debug('new log is empty, will start migrating')
      else debug('new log has %s msgs, will continue migrating', msgCountNewLog)

      let msgCountOldLog = 0
      pull(
        oldLogStream,
        skip(msgCountNewLog, function whenDoneSkipping(obj) {
          updateMigratedSize(obj)
          emitProgressEvent()
        }),
        pull.map(updateMigratedSizeAndPluck),
        pull.map(toBIPF),
        pull.asyncMap(writeTo(newLog)),
        drainMaybeGently(
          () => {
            msgCountOldLog++
          },
          (err) => {
            if (err) return console.error(err)
            debug('done migrating %s msgs from old log', msgCountOldLog)

            pull(
              oldLogStreamLive,
              pull.map(updateMigratedSizeAndPluck),
              pull.map(toBIPF),
              pull.asyncMap(writeTo(newLog)),
              pull.drain(() => {
                debug('1 new msg synced from old log to new log')
              })
            )
          }
        )
      )
    })
  }

  return {
    start,
    oldLogExists,
    // dangerouslyKillOldLog, // FIXME: implement this
  }
}
