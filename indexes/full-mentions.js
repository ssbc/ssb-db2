// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const bipf = require('bipf')
const pl = require('pull-level')
const pull = require('pull-stream')
const Plugin = require('./plugin')
const { or, seqs, liveSeqs } = require('../operators')

const BIPF_KEY = bipf.allocAndEncode('key')
const BIPF_CONTENT = bipf.allocAndEncode('content')
const BIPF_MENTIONS = bipf.allocAndEncode('mentions')

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
    const pKey = bipf.seekKey2(buf, 0, BIPF_KEY, 0)
    let p = 0 // note you pass in p!
    p = bipf.seekKey2(buf, pValue, BIPF_CONTENT, 0)
    if (p < 0) return
    p = bipf.seekKey2(buf, p, BIPF_MENTIONS, 0)
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
        // prettier-ignore
        if (err) return cb(new Error('FullMentions.getMessagesByMention() failed to read leveldb', {cause: err}))
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
