const bipf = require('bipf')
const pl = require('pull-level')
const pull = require('pull-stream')
const Plugin = require('./plugin')
const jsonCodec = require('flumecodec/json')
const { or, seqs, liveSeqs } = require('../operators')

// 1 index:
// - mentions (msgId) => msg offsets

const bKey = Buffer.from('key')
const bValue = Buffer.from('value')
const bContent = Buffer.from('content')
const bMentions = Buffer.from('mentions')

function parseInt10(x) {
  return parseInt(x, 10)
}

module.exports = class FullMentions extends Plugin {
  constructor(log, dir) {
    super(dir, 'fullMentions', 1)
    this.batch = []
  }

  handleData(record, seq) {
    if (record.offset < this.offset.value) return this.batch.length
    const recBuffer = record.value
    if (!recBuffer) return this.batch.length // deleted

    const pKey = bipf.seekKey(recBuffer, 0, bKey)

    let p = 0 // note you pass in p!
    p = bipf.seekKey(recBuffer, p, bValue)
    if (p < 0) return this.batch.length
    p = bipf.seekKey(recBuffer, p, bContent)
    if (p < 0) return this.batch.length
    p = bipf.seekKey(recBuffer, p, bMentions)
    if (p < 0) return this.batch.length
    const mentionsData = bipf.decode(recBuffer, p)
    if (!Array.isArray(mentionsData)) return this.batch.length
    const shortKey = bipf.decode(recBuffer, pKey).slice(1, 10)
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
    return this.batch.length
  }

  writeData(cb) {
    this.level.batch(this.batch, { keyEncoding: jsonCodec }, cb)
    this.batch = []
  }

  getResults(opts, live, cb) {
    pull(
      pl.read(this.level, opts),
      pull.collect((err, data) => {
        if (err) return cb(err)
        if (live) {
          const ps = pull(
            pl.read(this.level, Object.assign({}, opts, { live, old: false })),
            pull.map(parseInt10)
          )
          cb(null, or(seqs(data.map(parseInt10)), liveSeqs(ps))())
        } else cb(null, seqs(data.map(parseInt10)))
      })
    )
  }

  getMessagesByMention(key, live, cb) {
    this.getResults(
      {
        gte: [key, ''],
        lte: [key, undefined],
        keyEncoding: jsonCodec,
        keys: false,
      },
      live,
      cb
    )
  }
}
