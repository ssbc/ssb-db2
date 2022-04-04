// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const bipf = require('bipf')
const pl = require('pull-level')
const pull = require('pull-stream')
const clarify = require('clarify-error')
const Plugin = require('./plugin')
const { or, seqs, liveSeqs } = require('../operators')

const B_KEY = Buffer.from('key')
const B_CONTENT = Buffer.from('content')
const B_MENTIONS = Buffer.from('mentions')

function parseInt10(x) {
  return parseInt(x, 10)
}

// [destMsgId, origShortMsgId] => seq
module.exports = class FullMentions extends Plugin {
  constructor(log, dir) {
    super(log, dir, 'fullMentions', 1, 'json')
  }

  processRecord(record, seq, pValue) {
    const buf = record.value
    const pKey = bipf.seekKey(buf, 0, B_KEY)
    let p = 0 // note you pass in p!
    p = bipf.seekKey(buf, pValue, B_CONTENT)
    if (p < 0) return
    p = bipf.seekKey(buf, p, B_MENTIONS)
    if (p < 0) return
    const mentionsData = bipf.decode(buf, p)
    if (!Array.isArray(mentionsData)) return
    const shortKey = bipf.decode(buf, pKey).slice(1, 10)
    mentionsData.forEach((mention) => {
      if (
        mention.link &&
        typeof mention.link === 'string' &&
        (mention.link[0] === '@' || mention.link[0] === '%')
      ) {
        this.batch.push({
          type: 'put',
          key: [mention.link, shortKey],
          value: seq,
        })
      }
    })
  }

  indexesContent() {
    return true
  }

  getMessagesByMention(key, live, cb) {
    const opts = {
      gte: [key, ''],
      lte: [key, undefined],
      keyEncoding: this.keyEncoding,
      keys: false,
    }

    pull(
      pl.read(this.level, opts),
      pull.collect((err, seqArr) => {
        if (err) return cb(clarify(err, 'FullMentions.getMessagesByMention() failed to read leveldb')) // prettier-ignore
        if (live) {
          const ps = pull(
            pl.read(this.level, Object.assign({}, opts, { live, old: false })),
            pull.map(parseInt10)
          )
          cb(null, or(seqs(seqArr.map(parseInt10)), liveSeqs(ps)))
        } else cb(null, seqs(seqArr.map(parseInt10)))
      })
    )
  }
}
