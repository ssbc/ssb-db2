const bipf = require('bipf')
const Plugin = require('./plugin')
const { reEncrypt } = require('./private')

const B_VALUE = Buffer.from('value')
const B_AUTHOR = Buffer.from('author')
const B_SEQUENCE = Buffer.from('sequence')

// [author, sequence] => offset
module.exports = class EBT extends Plugin {
  constructor(log, dir) {
    super(log, dir, 'ebt', 1, 'json')
  }

  processRecord(record, seq) {
    const buf = record.value
    const pValue = bipf.seekKey(buf, 0, B_VALUE)
    if (pValue < 0) return
    const author = bipf.decode(buf, bipf.seekKey(buf, pValue, B_AUTHOR))
    const sequence = bipf.decode(buf, bipf.seekKey(buf, pValue, B_SEQUENCE))
    this.batch.push({
      type: 'put',
      key: [author, sequence],
      value: record.offset,
    })
  }

  indexesContent() {
    return false
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
