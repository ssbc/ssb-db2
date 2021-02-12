const bipf = require('bipf')
const Plugin = require('./plugin')
const { reEncrypt } = require('./private')

// 1 index:
// - [author, sequence] => offset

const bValue = Buffer.from('value')
const bAuthor = Buffer.from('author')
const bSequence = Buffer.from('sequence')

module.exports = class EBT extends Plugin {
  constructor(log, dir) {
    super(dir, 'ebt', 1)
    this.log = log
  }

  flushBatch(cb) {
    this.level.batch(this.batch, { keyEncoding: 'json' }, cb)
    this.batch = []
  }

  handleData(record, seq) {
    if (record.offset < this.offset.value) return
    const buf = record.value
    if (!buf) return // deleted

    const pValue = bipf.seekKey(buf, 0, bValue)
    if (pValue >= 0) {
      const author = bipf.decode(buf, bipf.seekKey(buf, pValue, bAuthor))
      const sequence = bipf.decode(buf, bipf.seekKey(buf, pValue, bSequence))
      this.batch.push({
        type: 'put',
        key: [author, sequence],
        value: record.offset,
      })
    }

    return
  }

  levelKeyToMessage(key, cb) {
    this.level.get(key, (err, offset) => {
      if (err) return cb(err)
      else
        this.log.get(parseInt(offset, 10), (err, record) => {
          if (err) return cb(err)
          cb(null, bipf.decode(record, 0))
        })
    })
  }

  // this is for EBT so must be careful to not leak private messages
  getMessageFromAuthorSequence(key, cb) {
    this.levelKeyToMessage(JSON.stringify(key), (err, msg) => {
      if (err) cb(err)
      else cb(null, reEncrypt(msg))
    })
  }
}
