// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const bipf = require('bipf')
const clarify = require('clarify-error')
const Plugin = require('./plugin')
const { reEncrypt } = require('./private')

// [author, sequence] => offset
module.exports = class EBT extends Plugin {
  constructor(log, dir) {
    super(log, dir, 'ebt', 1, 'json')
  }

  processRecord(record, seq) {
    const buf = record.value
    const pValue = bipf.seekKeyCached(buf, 0, 'value')
    if (pValue < 0) return
    const pValueAuthor = bipf.seekKeyCached(buf, pValue, 'author')
    if (pValueAuthor < 0) return
    const author = bipf.decode(buf, pValueAuthor)
    const pValueSequence = bipf.seekKeyCached(buf, pValue, 'sequence')
    if (pValueSequence < 0) return
    const sequence = bipf.decode(buf, pValueSequence)
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
      if (err) return cb(clarify(err, 'EBT.levelKeyToMessage() failed when getting leveldb item')) // prettier-ignore
      else
        this.log.get(parseInt(offset, 10), (err, record) => {
          if (err) return cb(clarify(err, 'EBT.levelKeyToMessage() failed when getting log record')) // prettier-ignore
          cb(null, bipf.decode(record, 0))
        })
    })
  }

  // this is for EBT so must be careful to not leak private messages
  getMessageFromAuthorSequence(key, cb) {
    this.levelKeyToMessage(JSON.stringify(key), (err, msg) => {
      if (err) cb(clarify(err, 'EBT.getMessageFromAuthorSequence() failed'))
      else cb(null, reEncrypt(msg))
    })
  }
}
