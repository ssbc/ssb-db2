// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const bipf = require('bipf')
const Plugin = require('./plugin')

const BIPF_KEY = bipf.allocAndEncode('key')

// msgId => seq
module.exports = class Keys extends Plugin {
  constructor(log, dir) {
    super(log, dir, 'keys', 1)
  }

  processRecord(record, seq, pValue) {
    const buf = record.value
    const pKey = bipf.seekKey2(buf, 0, BIPF_KEY, 0)
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

  getSeq(msgId, cb) {
    this.level.get(msgId, (err, seqStr) => {
      if (err) cb(err)
      else {
        const seq = parseInt(seqStr, 10)
        cb(null, seq)
      }
    })
  }

  delMsg(msgId) {
    this.level.del(msgId, (err) => {
      if (err) throw new Error('Keys.delMsg() failed', {cause: err})
    })
  }
}
