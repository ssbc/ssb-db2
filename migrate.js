const fs = require('fs')
const pull = require('pull-stream')
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
  if (sbot.createRawLogStream && sbot.status) {
    const logStream = sbot.createRawLogStream({ old: true, live: false })
    const logStreamLive = sbot.createRawLogStream({ old: false, live: true })
    const getSize = () => sbot.status().sync.since
    return [logStream, logStreamLive, getSize]
  }
  if (sbot.createLogStream && sbot.status) {
    const logStream = sbot.createLogStream({
      raw: true,
      old: true,
      live: false,
    })
    const logStreamLive = sbot.createLogStream({
      raw: true,
      old: false,
      live: true,
    })
    const getSize = () => sbot.status().sync.since
    return [logStream, logStreamLive, getSize]
  } else {
    const oldLog = FlumeLog(oldLogPath(config.path), {
      blockSize: BLOCK_SIZE,
      codec: jsonCodec,
    })
    const opts = {
      keys: true,
      seqs: true,
      value: true,
      sync: false,
      codec: jsonCodec,
    }
    const logStream = oldLog.stream({ old: true, live: false, ...opts })
    const logStreamLive = oldLog.stream({ old: false, live: true, ...opts })
    const getSize = () => oldLog.since.value
    return [logStream, logStreamLive, getSize]
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

exports.version = '0.4.0'

exports.manifest = {
  start: 'sync',
  doesOldLogExist: 'sync',
}

exports.init = function init(sbot, config) {
  const oldLogExists = makeFileExistsObv(oldLogPath(config.path))

  let started = false
  let hasCloseHook = false
  let retryPeriod = 250
  let retryTimer
  let liveDebugTimer
  let drainAborter = null

  function oldLogMissingThenRetry(fn) {
    if (!hasCloseHook) {
      sbot.close.hook(function (fn, args) {
        clearTimeout(retryTimer)
        clearInterval(liveDebugTimer)
        if (drainAborter) drainAborter.abort()
        fn.apply(this, args)
      })
      hasCloseHook = true
    }
    oldLogExists.set(fileExists(oldLogPath(config.path)))
    if (oldLogExists.value === false) {
      retryPeriod = Math.min(retryPeriod * 2, 8000)
      retryTimer = setTimeout(fn, retryPeriod)
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
    if (oldLogMissingThenRetry(start)) return
    started = true
    debug('started')

    const [oldLogStream, oldLogStreamLive, getOldSize] = getOldLogStreams(
      sbot,
      config
    )
    const newLog =
      sbot.db && sbot.db.getLog() && sbot.db.getLog().stream
        ? sbot.db.getLog()
        : AsyncFlumeLog(newLogPath(config.path), { blockSize: BLOCK_SIZE })
    const newLogStream = newLog.stream({ gte: 0 })

    let migratedSize = null
    let progressCalls = 0

    function updateMigratedSize(obj) {
      migratedSize = obj.seq
    }

    function updateMigratedSizeAndPluck(obj) {
      updateMigratedSize(obj)
      return obj.value
    }

    function emitProgressEvent() {
      const oldSize = getOldSize()
      if (oldSize > 0 && migratedSize !== null) {
        const progress = migratedSize / oldSize
        if (
          progress === 1 ||
          progressCalls < 100 ||
          progressCalls++ % 1000 === 0
        ) {
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
        (drainAborter = pull.drain(
          () => {
            msgCountOldLog++
          },
          (err) => {
            if (err) return console.error(err)
            debug('done migrating %s msgs from old log', msgCountOldLog)

            let liveMsgCount = 0
            if (debug.enabled) {
              liveDebugTimer = setInterval(() => {
                if (liveMsgCount === 0) return
                debug('%d msgs synced from old log to new log', liveMsgCount)
                liveMsgCount = 0
              }, 2000)
            }

            pull(
              oldLogStreamLive,
              pull.map(updateMigratedSizeAndPluck),
              pull.map(toBIPF),
              pull.asyncMap(writeTo(newLog)),
              (drainAborter = pull.drain(() => {
                liveMsgCount++
              }))
            )
          }
        ))
      )
    })
  }

  return {
    start,
    doesOldLogExist: () => oldLogExists.value,
    // dangerouslyKillOldLog, // FIXME: implement this
  }
}
