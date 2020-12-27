const bipf = require('bipf')
const pl = require('pull-level')
const pull = require('pull-stream')
const Plugin = require('./plugin')

// 1 index:
// - author => latest { msg key, sequence timestamp } (validate state & EBT)

module.exports = function (log, dir, private) {
  const bValue = Buffer.from('value')
  const bKey = Buffer.from('key')
  const bAuthor = Buffer.from('author')
  const bSequence = Buffer.from('sequence')
  const bTimestamp = Buffer.from('timestamp')

  const throwOnError = function (err) {
    if (err) throw err
  }

  let batch = []
  let authorLatest = {}
  const META = '\x00'

  const { level, offset, stateLoaded, onData, writeBatch } = Plugin(
    dir,
    'base',
    1,
    handleData,
    writeData,
    beforeIndexUpdate
  )

  function writeData(cb) {
    level.batch(batch, { valueEncoding: 'json' }, (err) => {
      if (err) return cb(err)
      else private.saveIndexes(cb)
    })

    batch = []
  }

  function handleData(record, processed) {
    if (record.offset < offset.value) return batch.length
    if (!record.value) return batch.length // deleted

    let p = 0 // note you pass in p!
    const pKey = bipf.seekKey(record.value, p, bKey)

    p = 0
    p = bipf.seekKey(record.value, p, bValue)
    if (~p) {
      const p2 = bipf.seekKey(record.value, p, bAuthor)
      const author = bipf.decode(record.value, p2)
      const p3 = bipf.seekKey(record.value, p, bSequence)
      const sequence = bipf.decode(record.value, p3)
      const p4 = bipf.seekKey(record.value, p, bTimestamp)
      const timestamp = bipf.decode(record.value, p4)

      let latestSequence = 0
      if (authorLatest[author]) latestSequence = authorLatest[author].sequence
      if (sequence > latestSequence) {
        const key = bipf.decode(record.value, pKey)
        authorLatest[author] = { id: key, sequence, timestamp }
        batch.push({
          type: 'put',
          key: author,
          value: authorLatest[author],
        })
      }
    }

    return batch.length
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
        gt: META,
        valueEncoding: 'json',
      }),
      pull.collect((err, data) => {
        if (err) return cb(err)
        const result = {}
        data.forEach((d) => {
          result[d.key] = d.value
        })
        cb(null, result)
      })
    )
  }

  return {
    offset,
    stateLoaded,
    onData,
    writeBatch,

    remove: level.clear,
    close: level.close.bind(level),

    // returns { id (msg key), sequence, timestamp }
    getLatest: function (feedId, cb) {
      level.get(feedId, { valueEncoding: 'json' }, cb)
    },
    getAllLatest,
    removeFeedFromLatest: function (feedId) {
      level.del(feedId, throwOnError)
    },
  }
}
