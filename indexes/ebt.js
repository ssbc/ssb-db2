const bipf = require('bipf')
const Plugin = require('./plugin')
const { reEncrypt } = require('./private')

// 1 index:
// - [author, sequence] => offset

module.exports = function (log, dir) {
  const bValue = Buffer.from('value')
  const bAuthor = Buffer.from('author')
  const bSequence = Buffer.from('sequence')

  let batch = []

  const name = 'ebt'
  const { level, offset, stateLoaded, onData, writeBatch } = Plugin(
    dir,
    name,
    1,
    handleData,
    writeData
  )

  function writeData(cb) {
    level.batch(batch, { keyEncoding: 'json' }, cb)
    batch = []
  }

  function handleData(record, processed) {
    if (record.offset < offset.value) return batch.length
    const buf = record.value
    if (!buf) return batch.length // deleted

    const pValue = bipf.seekKey(buf, 0, bValue)
    if (pValue >= 0) {
      const author = bipf.decode(buf, bipf.seekKey(buf, pValue, bAuthor))
      const sequence = bipf.decode(buf, bipf.seekKey(buf, pValue, bSequence))
      batch.push({
        type: 'put',
        key: [author, sequence],
        value: record.offset,
      })
    }

    return batch.length
  }

  function levelKeyToMessage(key, cb) {
    level.get(key, (err, offset) => {
      if (err) return cb(err)
      else
        log.get(parseInt(offset, 10), (err, record) => {
          if (err) return cb(err)
          cb(null, bipf.decode(record, 0))
        })
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
