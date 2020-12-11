const bipf = require('bipf')
const Plugin = require('./plugin')
const { reEncrypt } = require('./private')

// 1 index:
// - [author, sequence] => seq (EBT)

module.exports = function (log, dir) {
  const bValue = Buffer.from('value')
  const bAuthor = Buffer.from('author')
  const bSequence = Buffer.from('sequence')

  let batch = []

  const name = 'ebt'
  const { level, seq, stateLoaded, onData, writeBatch } = Plugin(
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

  function handleData(data, processed) {
    if (data.seq < seq.value) return

    let p = 0 // note you pass in p!
    p = bipf.seekKey(data.value, p, bValue)
    if (~p) {
      const p2 = bipf.seekKey(data.value, p, bAuthor)
      const author = bipf.decode(data.value, p2)
      const p3 = bipf.seekKey(data.value, p, bSequence)
      const sequence = bipf.decode(data.value, p3)

      batch.push({
        type: 'put',
        key: [author, sequence],
        value: data.seq,
      })
    }

    return batch.length
  }

  function levelKeyToMessage(key, cb) {
    level.get(key, (err, seq) => {
      if (err) return cb(err)
      else
        log.get(parseInt(seq, 10), (err, data) => {
          if (err) return cb(err)
          cb(null, bipf.decode(data, 0))
        })
    })
  }

  return {
    seq,
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
