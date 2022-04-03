// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const bipf = require('bipf')
const clarify = require('clarify-error')
const Plugin = require('./plugin')
const { seqs } = require('../operators')

const B_KEY = Buffer.from('key')

// msgId => seq
module.exports = class Keys extends Plugin {
  constructor(log, dir) {
    super(log, dir, 'keys', 1)
  }

  processRecord(record, seq, pValue) {
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

  indexesContent() {
    return false
  }

  getMsgByKey(msgId, cb) {
    this.level.get(msgId, (err, seqNum) => {
      if (err) cb(null, seqs([]))
      else cb(null, seqs([parseInt(seqNum, 10)]))
    })
  }

  getSeq(msgId, cb) {
    this.level.get(msgId, cb)
  }

  delMsg(msgId) {
    this.level.del(msgId, (err) => {
      if (err) throw clarify(err, 'Keys.delMsg() failed')
    })
  }
}
