const path = require('path')
const pull = require('pull-stream')
const FlumeLog = require('flumelog-offset')
const bipf = require('bipf')
const jsonCodec = require('flumecodec/json')
const debug = require('debug')('ssb:db2:migration')

const blockSize = 64 * 1024

function skip(count) {
  let skipped = 0
  return pull.filter(() => {
    if (skipped >= count) return true
    else {
      skipped++
      return false
    }
  })
}

function getOldLogStream(sbot, config, live) {
  const old = !live
  if (sbot.createRawLogStream) {
    return pull(
      sbot.createRawLogStream({ old, live }),
      pull.map((obj) => obj.value)
    )
  } else {
    const oldLogPath = path.join(config.path, 'flume', 'log.offset')
    const oldLog = FlumeLog(oldLogPath, { blockSize, codec: jsonCodec })
    return pull(oldLog.stream({ seqs: false, old, live, codec: jsonCodec }))
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

exports.init = function init(sbot, config) {
  const oldLogStream = getOldLogStream(sbot, config, false)
  const oldLogStreamLive = getOldLogStream(sbot, config, true)
  const newLogStream = sbot.db.log.stream({ gte: 0 })

  let dataTransferred = 0 // FIXME: does this only work if the new log is empty?
  function writeTo(log) {
    return (data, cb) => {
      dataTransferred += data.length
      // FIXME: could we use log.add since it already converts to BIPF?
      // FIXME: see also issue #16
      log.append(data, () => {})
      if (dataTransferred % blockSize == 0) log.onDrain(cb)
      else cb()
    }
  }

  scanAndCount(newLogStream, (err, msgCountNewLog) => {
    if (msgCountNewLog === 0) debug('new log is empty, will start migrating')
    else debug('new log has %s msgs, will continue migrating', msgCountNewLog)

    pull(
      oldLogStream,
      skip(msgCountNewLog),
      pull.map(toBIPF),
      pull.asyncMap(writeTo(sbot.db.log)),
      pull.reduce(
        (x) => x + 1,
        0,
        (err, msgCountOldLog) => {
          if (err) return console.error(err)
          debug('done migrating %s msgs from old log', msgCountOldLog)

          pull(
            oldLogStreamLive,
            pull.map(toBIPF),
            pull.asyncMap(writeTo(sbot.db.log)),
            pull.drain(() => {
              debug('1 new msg synced from old log to new log')
            })
          )
        }
      )
    )
  })
}
