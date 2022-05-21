// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const bipf = require('bipf')
const clarify = require('clarify-error')

const Plugin = require('./plugin')
const { reEncrypt } = require('./private')
const SSBURI = require('ssb-uri2')

const BIPF_AUTHOR = bipf.allocAndEncode('author')
const BIPF_PARENT = bipf.allocAndEncode('parent')
const BIPF_SEQUENCE = bipf.allocAndEncode('sequence')

// [author, sequence] => offset
module.exports = class EBT extends Plugin {
  constructor(log, dir) {
    super(log, dir, 'ebt', 1, 'json')
  }

  processRecord(record, seq, pValue) {
    const buf = record.value
    let author = bipf.decode(buf, bipf.seekKey2(buf, pValue, BIPF_AUTHOR, 0))
    if (SSBURI.isButtwooV1FeedSSBURI(author)) {
      const parent = bipf.decode(buf, bipf.seekKey2(buf, pValue, BIPF_PARENT, 0))
      author += parent === null ? '' : parent
    }
    const sequence = bipf.decode(buf, bipf.seekKey2(buf, pValue, BIPF_SEQUENCE, 0))
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
      // prettier-ignore
      if (err) return cb(clarify(err, 'EBT.levelKeyToMessage() failed when getting leveldb item'))
      else
        this.log.getRaw(parseInt(offset, 10), (err, record) => {
          // prettier-ignore
          if (err) return cb(clarify(err, 'EBT.levelKeyToMessage() failed when getting log record'))
          cb(null, record)
        })
    })
  }

  // this is for EBT so must be careful to not leak private messages
  getMessageFromAuthorSequence(key, cb) {
    this.levelKeyToMessage(JSON.stringify(key), (err, record) => {
      if (err) cb(clarify(err, 'EBT.getMessageFromAuthorSequence() failed'))
      else cb(null, bipf.decode(record, 0))
    })
  }

  getMessageFromAuthorSequenceRaw(key, cb) {
    this.levelKeyToMessage(JSON.stringify(key), (err, record) => {
      if (err) cb(clarify(err, 'EBT.getMessageFromAuthorSequence() failed'))
      else cb(null, record)
    })
  }
}
