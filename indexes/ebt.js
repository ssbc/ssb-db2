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
    const recOffset = record.seq // "seq" is abstract, here means "offset"
    const recBuffer = record.value

    if (recOffset < offset.value) return
    if (!recBuffer) return // deleted

    let p = 0 // note you pass in p!
    p = bipf.seekKey(recBuffer, p, bValue)
    if (~p) {
      const p2 = bipf.seekKey(recBuffer, p, bAuthor)
      const author = bipf.decode(recBuffer, p2)
      const p3 = bipf.seekKey(recBuffer, p, bSequence)
      const sequence = bipf.decode(recBuffer, p3)

      batch.push({
        type: 'put',
        key: [author, sequence],
        value: recOffset,
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
