const bipf = require('bipf')
const pl = require('pull-level')
const pull = require('pull-stream')
const Plugin = require('./plugin')

// 3 indexes:
// - msg key => seq
// - [author, sequence] => seq (EBT)
// - author => latest { msg key, sequence timestamp } (validate state & EBT)

module.exports = function (log, dir) {
  const bValue = Buffer.from('value')
  const bKey = Buffer.from('key')
  const bAuthor = Buffer.from('author')
  const bSequence = Buffer.from('sequence')
  const bTimestamp = Buffer.from('timestamp')

  const throwOnError = function (err) {
    if (err) throw err
  }

  let batchBasic = []
  let batchJsonKey = []
  let batchJson = []
  let authorLatest = {}

  const { level, seq } = Plugin(
    log,
    dir,
    'base',
    1,
    handleData,
    writeData,
    beforeIndexUpdate
  )

  function writeData(cb) {
    level.batch(batchBasic, throwOnError)
    level.batch(batchJsonKey, { keyEncoding: 'json' }, throwOnError)
    level.batch(batchJson, { keyEncoding: 'json', valueEncoding: 'json' }, cb)

    batchBasic = []
    batchJsonKey = []
    batchJson = []
  }

  function handleData(data) {
    let p = 0 // note you pass in p!
    p = bipf.seekKey(data.value, p, bKey)
    const key = bipf.decode(data.value, p)
    batchBasic.push({ type: 'put', key, value: data.seq })

    p = 0
    p = bipf.seekKey(data.value, p, bValue)
    if (~p) {
      const p2 = bipf.seekKey(data.value, p, bAuthor)
      const author = bipf.decode(data.value, p2)
      const p3 = bipf.seekKey(data.value, p, bSequence)
      const sequence = bipf.decode(data.value, p3)
      const p4 = bipf.seekKey(data.value, p, bTimestamp)
      const timestamp = bipf.decode(data.value, p4)

      batchJsonKey.push({
        type: 'put',
        key: [author, sequence],
        value: data.seq,
      })

      let latestSequence = 0
      if (authorLatest[author]) latestSequence = authorLatest[author].sequence
      if (sequence > latestSequence) {
        authorLatest[author] = { id: key, sequence, timestamp }
        batchJson.push({
          type: 'put',
          key: ['a', author],
          value: authorLatest[author],
        })
      }
    }

    if (batchBasic.length) return batchBasic.length
    else return 0
  }

  function beforeIndexUpdate(cb) {
    getAllLatest((err, latest) => {
      authorLatest = latest
      cb()
    })
  }

  function getAllLatest(cb) {
    pull(
      pl.read(level, {
        gte: '["a",',
        valueEncoding: 'json',
      }),
      pull.collect((err, data) => {
        if (err) return cb(err)
        const result = {}
        data.forEach((d) => {
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

  const self = {
    seq,
    remove: level.clear,
    close: level.close.bind(level),

    getMessageFromKey: levelKeyToMessage,
    getMessageFromAuthorSequence: (key, cb) => {
      levelKeyToMessage(JSON.stringify(key), cb)
    },

    // returns { id (msg key), sequence, timestamp }
    getLatest: function (feedId, cb) {
      level.get(
        ['a', feedId],
        { keyEncoding: 'json', valueEncoding: 'json' },
        cb
      )
    },
    getAllLatest,
    keyToSeq: level.get, // used by delete
    removeFeedFromLatest: function (feedId) {
      level.del(['a', feedId], { keyEncoding: 'json' }, throwOnError)
    },
  }

  return self
}
