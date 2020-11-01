const bipf = require('bipf')
const Obv = require('obv')
const path = require('path')
const Level = require('level')
const pl = require('pull-level')
const pull = require('pull-stream')
const debug = require('debug')("base-index")

// 3 indexes:
// - msg key => seq
// - [author, sequence] => seq (EBT)
// - author => latest { msg key, sequence timestamp } (validate state & EBT)

module.exports = function (log, dir, feedId) {
  var seq = Obv()
  seq.set(-1)

  // FIXME: mkdirp

  var level = Level(path.join(dir, "indexes", "base"))
  const META = '\x00'
  const version = 1

  const bValue = Buffer.from('value')
  const bKey = Buffer.from('key')
  const bAuthor = Buffer.from('author')
  const bSequence = Buffer.from('sequence')
  const bTimestamp = Buffer.from('timestamp')

  const throwOnError = function(err) { if (err) throw err }

  var batchBasic = []
  var batchJsonKey = []
  var batchJson = []

  const chunkSize = 512
  var processed = 0

  function writeBatch() {
    level.put(META, { version, seq: seq.value },
              { valueEncoding: 'json' })

    level.batch(batchBasic, throwOnError)
    level.batch(batchJsonKey, { keyEncoding: 'json' }, throwOnError)
    level.batch(batchJson, { keyEncoding: 'json', valueEncoding: 'json' }, throwOnError)

    batchBasic = []
    batchJsonKey = []
    batchJson = []
  }

  let authorLatest = {}

  function handleData(data) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(data.value, p, bKey)
    const key = bipf.decode(data.value, p)
    batchBasic.push({ type: 'put', key, value: data.seq })

    p = 0
    p = bipf.seekKey(data.value, p, bValue)
    if (~p) {
      var p2 = bipf.seekKey(data.value, p, bAuthor)
      const author = bipf.decode(data.value, p2)
      var p3 = bipf.seekKey(data.value, p, bSequence)
      const sequence = bipf.decode(data.value, p3)
      var p4 = bipf.seekKey(data.value, p, bTimestamp)
      const timestamp = bipf.decode(data.value, p4)

      batchJsonKey.push({ type: 'put', key: [author, sequence], value: data.seq })
      
      var latestSequence = 0
      if (authorLatest[author])
        latestSequence = authorLatest[author].sequence
      if (sequence > latestSequence) {
        authorLatest[author] = { id: key, sequence, timestamp }
        batchJson.push({ type: 'put', key: ['a', author], value: authorLatest[author] })
      }
    }

    seq.set(data.seq)
    processed++

    // FIXME: live stream
    if (batchBasic.length > chunkSize)
      writeBatch()
  }

  function updateIndexes() {
    const start = Date.now()

    // FIXME: new messages from user should call updateClock on ebt

    log.stream({ gt: seq.value }).pipe({
      paused: false,
      write: handleData,
      end: () => {
        if (batchBasic.length > 0)
          writeBatch()

        debug(`base index scan time: ${Date.now()-start}ms, items: ${processed}`)

        log.stream({ gt: seq.value, live: true }).pipe({
          paused: false,
          write: handleData
        })
      }
    })
  }

  level.get(META, { valueEncoding: 'json' }, (err, data) => {
    debug("got base index status:", data)

    if (data && data.version == version) {
      seq.set(data.seq)
      getAllLatest((err, latest) => {
        authorLatest = latest
        updateIndexes()
      })
    } else
      level.clear(updateIndexes)
  })

  function getAllLatest(cb) {
    console.time("get all latest")
    pull(
      pl.read(level, {
        gte: '["a",',
        valueEncoding: 'json'
      }),
      pull.collect((err, data) => {
        if (err) return cb(err)
        let result = {}
        data.forEach(d => {
          result[JSON.parse(d.key)[1]] = d.value
        })
        console.log(Object.keys(result).length)
        console.timeEnd("get all latest")
        cb(null, result)
      })
    )
  }
  
  function levelKeyToMessage(key, cb) {
    level.get(key, (err, seq) => {
      if (err) return cb(err)
      else
        log.get(seq, (err, data) => {
          if (err) return cb(err)
          cb(null, bipf.decode(data, 0))
        })
    })
  }
  
  var self = {
    getMessageFromKey: levelKeyToMessage,
    getMessageFromAuthorSequence: levelKeyToMessage,

    // returns { id (msg key), sequence, timestamp }
    getLatest: function(feedId, cb) {
      level.get(['a', feedId],
                { keyEncoding: 'json', valueEncoding: 'json' },
                cb)
    },
    getAllLatest,
    seq,
    keyToSeq: level.get, // used by delete
    removeFeedFromLatest: function(feedId) {
      level.del(['a', feedId], { keyEncoding: 'json' }, throwOnError)
    },
    remove: level.clear
  }

  // ssb-db compatibility
  self.getAtSequence = self.getDataFromAuthorSequence
  // FIXME: self.getVectorClock
  
  return self
}
