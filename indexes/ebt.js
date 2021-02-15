const bipf = require('bipf')
const pull = require('pull-stream')
const pl = require('pull-level')
const Plugin = require('./plugin')
const jsonCodec = require('flumecodec/json')
const { reEncrypt } = require('./private')

// 1 index:
// - [author, sequence] => offset

module.exports = function (log, dir) {
  const bValue = Buffer.from('value')
  const bAuthor = Buffer.from('author')
  const bSequence = Buffer.from('sequence')

  let batch = []
  // it turns out that if you place the same key in a batch multiple
  // times. Level will happily write that key as many times as you give
  // it, instead of just writing the last value for the key, so we have
  // to help the poor bugger
  let batchKeys = {} // key to index

  // a map of feed -> { sequence: offset, ... }
  const feedValues = {}

  const name = 'ebt'
  const { level, offset, stateLoaded, onData, writeBatch } = Plugin(
    dir,
    name,
    1,
    handleData,
    writeData,
    beforeIndexUpdate
  )

  function writeData(cb) {
    level.batch(batch, { valueEncoding: 'json' }, cb)
    batch = []
    batchKeys = {}
  }

  function handleData(record, processed) {
    const buf = record.value
    if (!buf) return batch.length // deleted

    const pValue = bipf.seekKey(buf, 0, bValue)
    if (pValue >= 0) {
      const author = bipf.decode(buf, bipf.seekKey(buf, pValue, bAuthor))
      const sequence = bipf.decode(buf, bipf.seekKey(buf, pValue, bSequence))

      const values = feedValues[author] || {}
      values[sequence] = record.offset
      feedValues[author] = values

      const batchValue = {
        type: 'put',
        key: author,
        value: values,
      }

      let existingKeyIndex = batchKeys[author]
      if (existingKeyIndex) {
        batch[existingKeyIndex] = batchValue
      }
      else {
        batch.push(batchValue)
        batchKeys[author] = batch.length - 1
      }
    }

    return batch.length
  }

  function beforeIndexUpdate(cb) {
    console.time("getting ebt state")
    pull(
      pl.read(level, {
        valueEncoding: jsonCodec,
        keys: true
      }),
      pull.collect((err, data) => {
        if (err) return cb(err)

        for (var i = 0; i < data.length; ++i) {
          const feedValue = data[i]
          feedValues[feedValue.key] = feedValue.value
        }

        console.timeEnd("getting ebt state")

        cb()
      })
    )
  }

  function levelKeyToMessage(key, cb) {
    const parsedKey = JSON.parse(key)
    const values = feedValues[parsedKey[0]]
    log.get(values[parsedKey[1].toString()], (err, record) => {
      if (err) return cb(err)
      cb(null, bipf.decode(record, 0))
    })
  }

  return {
    offset,
    stateLoaded,
    onData,
    writeBatch,
    name,

    remove: level.clear,
    close: level.close.bind(level),

    // this is for EBT so must be not leak private messages
    getMessageFromAuthorSequence: (key, cb) => {
      levelKeyToMessage(JSON.stringify(key), (err, msg) => {
        if (err) cb(err)
        else cb(null, reEncrypt(msg))
      })
    },
  }
}
