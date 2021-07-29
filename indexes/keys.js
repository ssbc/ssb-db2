const bipf = require('bipf')
const Plugin = require('./plugin')
const { seqs } = require('../operators')

const B_KEY = Buffer.from('key')

// msgId => seq
module.exports = class Keys extends Plugin {
  constructor(log, dir) {
    super(log, dir, 'keys', 1)
  }

  processRecord(record, seq) {
    const buf = record.value
    const pKey = bipf.seekKey(buf, 0, B_KEY)
    if (pKey < 0) return
    const key = bipf.decode(buf, pKey)
    this.batch.push({
      type: 'put',
      key: key,
      value: seq,
    })
  }

  getMsgByKey(msgId, cb) {
    this.level.get(msgId, (err, seqNum) => {
      if (err) cb(null, seqs([]))
      else cb(null, seqs([parseInt(seqNum, 10)]))
    })
  }

  delMsg(msgId) {
    this.level.del(msgId, (err) => {
      if (err) throw err
    })
  }
}
