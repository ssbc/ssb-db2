const bipf = require('bipf')
const Obv = require('obv')
const path = require('path')
const Level = require('level')
const pl = require('pull-level')
const pull = require('pull-stream')
const debug = require('debug')

// 3 indexes:
// - msg key => seq
// - [author, sequence] => seq (EBT)
// - author => latest { msg key, sequence timestamp } (validate state & EBT)

module.exports = function (log, dir, feedId) {
  var seq = Obv()
  seq.set(-1)

  var level = Level(path.join(dir, "indexes", "base"))
  const META = '\x00'
  const version = 1

  const bValue = Buffer.from('value')
  const bKey = Buffer.from('key')
  const bAuthor = Buffer.from('author')
  const bSequence = Buffer.from('sequence')
  const bTimestamp = Buffer.from('timestamp')

  const throwOnError = function(err) { if (err) throw err }

  var batch = level.batch()
  const chunkSize = 512
  var messagesProcessed = 0

  function writeBatch() {
    batch.put(META, { version, seq: data.seq },
              { valueEncoding: 'json' }, throwOnError)
    batch.write()
  }

  let authorLatest = {}
  
  function handleData(data) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(data.value, p, bKey)
    const key = bipf.decode(data.value, p)
    batch.put(key, data.seq, throwOnError)

    p = 0
    p = bipf.seekKey(data.value, p, bValue)
    if (~p) {
      var p2 = bipf.seekKey(data.value, p, bAuthor)
      const author = bipf.decode(data.value, p2)
      var p3 = bipf.seekKey(data.value, p, bSequence)
      const sequence = bipf.decode(data.value, p3)
      var p4 = bipf.seekKey(data.value, p, bTimestamp)
      const timestamp = bipf.decode(data.value, p4)

      batch.put([author, sequence], data.seq,
                { keyEncoding: 'json' }, throwOnError)
      
      var latestSequence = 0
      if (authorLatest[author])
        latestSequence = authorLatest[author].sequence
      if (sequence > latestSequence) {
        authorLatest[author] = { id: key, sequence, timestamp }
        batch.put(['author', author],
                  authorLatest[author],
                  { keyEncoding: 'json', valueEncoding: 'json' },
                  throwOnError)
      }
    }

    seq.set(data.seq)
    messagesProcessed++

    if (batch.length > chunkSize)
      writeBatch()
  }

  function updateIndexes() {
    const start = Date.now()

    // FIXME: new messages from user should call updateClock on ebt

    log.stream({ gt: seq.value }).pipe({
      paused: false,
      write: handleData,
      end: () => {
        if (batch.length > 0)
          writeBatch()
        
        debug(`key index full scan time: ${Date.now()-start}ms, total items: ${messagesProcessed}`)

        log.stream({ gt: seq.value, live: true }).pipe({
          paused: false,
          write: handleData
        })
      }
    })
  }
  
  level.get(META, (err, data) => {
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
    pull(
      pl.read(level, {
        gte: '["author',
        valueEncoding: 'json'
      }),
      pull.collect((err, data) => {
        if (err) return cb(err)
        let result = {}
        data.forEach(d => {
          result[JSON.parse(d.key)[1]] = d.value
        })
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
      level.get(['author', feedId],
                { keyEncoding: 'json', valueEncoding: 'json' },
                cb)
    },
    getAllLatest,
    seq,
    keyToSeq: level.get, // used by delete
    removeFeedFromLatest: function(feedId) {
      level.del(['author', feedId], { keyEncoding: 'json' }, throwOnError)
    },
    remove: level.clear
  }

  // ssb-db compatibility
  self.getAtSequence = self.getDataFromAuthorSequence
  // FIXME: self.getVectorClock
  
  return self
}
