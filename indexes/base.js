const bipf = require('bipf')
const pl = require('pull-level')
const pull = require('pull-stream')
const Plugin = require('./plugin')

// 1 index:
// - author => latest { msg key, sequence timestamp } (validate state & EBT)

module.exports = function (log, dir, privateIndex) {
  const bKey = Buffer.from('key')
  const bValue = Buffer.from('value')
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
      else privateIndex.saveIndexes(cb)
    })

    batch = []
  }

  function handleData(record, processed) {
    const buf = record.value
    if (!buf) return batch.length // deleted

    const pValue = bipf.seekKey(buf, 0, bValue)
    if (pValue >= 0) {
      const author = bipf.decode(buf, bipf.seekKey(buf, pValue, bAuthor))
      const sequence = bipf.decode(buf, bipf.seekKey(buf, pValue, bSequence))
      const timestamp = bipf.decode(buf, bipf.seekKey(buf, pValue, bTimestamp))

      let latestSequence = 0
      if (authorLatest[author]) latestSequence = authorLatest[author].sequence
      if (sequence > latestSequence) {
        const key = bipf.decode(buf, bipf.seekKey(buf, 0, bKey))
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
