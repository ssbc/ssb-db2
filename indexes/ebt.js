// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const bipf = require('bipf')
const clarify = require('clarify-error')
const Plugin = require('./plugin')

const BIPF_AUTHOR = bipf.allocAndEncode('author')
const BIPF_FEED = bipf.allocAndEncode('feed')
const BIPF_SEQUENCE = bipf.allocAndEncode('sequence')

// [feedId, sequence] => offset
module.exports = class EBT extends Plugin {
  constructor(log, dir) {
    super(log, dir, 'ebt', 1, 'json')
  }

  processRecord(record, seq, pValue) {
    const buf = record.value
    const pValueAuthor = bipf.seekKey2(buf, pValue, BIPF_AUTHOR, 0)
    const author = bipf.decode(buf, pValueAuthor)
    const pValueSequence = bipf.seekKey2(buf, pValue, BIPF_SEQUENCE, 0)
    const sequence = bipf.decode(buf, pValueSequence)
    const pFeed = bipf.seekKey2(buf, 0, BIPF_FEED, 0)
    const feedId = pFeed < 0 ? author : bipf.decode(buf, pFeed)
    this.batch.push({
      type: 'put',
      key: [feedId, sequence],
      value: record.offset,
    })
  }

  indexesContent() {
    return false
  }

  levelKeyToRecord(key, cb) {
    this.level.get(key, (err, offset) => {
      // prettier-ignore
      if (err) return cb(clarify(err, 'EBT.levelKeyToRecord() failed when getting leveldb item'))
      else
        this.log.getRaw(parseInt(offset, 10), (err, buffer) => {
          // prettier-ignore
          if (err) return cb(clarify(err, 'EBT.levelKeyToRecord() failed when getting log record'))
          cb(null, buffer)
        })
    })
  }

  levelKeyToNativeMsg(key, cb) {
    this.level.get(key, (err, offset) => {
      // prettier-ignore
      if (err) return cb(clarify(err, 'EBT.levelKeyToNativeMsg() failed when getting leveldb item'))
      else
        this.log.getNativeMsg(parseInt(offset, 10), (err, nativeMsg) => {
          // prettier-ignore
          if (err) return cb(clarify(err, 'EBT.levelKeyToNativeMsg() failed when getting log record'))
          cb(null, nativeMsg)
        })
    })
  }

  // this is for EBT so must be careful to not leak private messages
  getMessageFromAuthorSequence(key, cb) {
    this.levelKeyToRecord(JSON.stringify(key), (err, buffer) => {
      if (err) cb(clarify(err, 'EBT.getMessageFromAuthorSequence() failed'))
      else cb(null, bipf.decode(buffer, 0))
    })
  }

  // this is for EBT so must be careful to not leak private messages
  getMessageFromAuthorSequenceNativeMsg(key, cb) {
    this.levelKeyToNativeMsg(JSON.stringify(key), (err, nativeMsg) => {
      // prettier-ignore
      if (err) cb(clarify(err, 'EBT.getMessageFromAuthorSequenceNativeMsg() failed'))
      else cb(null, nativeMsg)
    })
  }
}
