const bipf = require('bipf')
const pl = require('pull-level')
const pull = require('pull-stream')
const Plugin = require('./plugin')
const jsonCodec = require('flumecodec/json')
const { or, seqs, liveSeqs } = require('../operators')

// 1 index:
// - mentions (msgId) => msg offsets

module.exports = function (log, dir) {
  const bKey = Buffer.from('key')
  const bValue = Buffer.from('value')
  const bContent = Buffer.from('content')
  const bMentions = Buffer.from('mentions')

  let batch = []

  const name = 'fullMentions'
  const { level, offset, stateLoaded, onData, writeBatch } = Plugin(
    dir,
    name,
    1,
    handleData,
    writeData
  )

  function writeData(cb) {
    level.batch(batch, { keyEncoding: jsonCodec }, cb)
    batch = []
  }

  function handleData(record, processed) {
    if (record.offset < offset.value) return
    if (!record.value) return // deleted

    let p = 0 // note you pass in p!
    const pKey = bipf.seekKey(record.value, p, bKey)

    p = 0
    p = bipf.seekKey(record.value, p, bValue)
    if (~p) {
      const pContent = bipf.seekKey(record.value, p, bContent)
      if (~pContent) {
        const pMentions = bipf.seekKey(record.value, pContent, bMentions)
        if (~pMentions) {
          const mentionsData = bipf.decode(record.value, pMentions)
          if (Array.isArray(mentionsData)) {
            const shortKey = bipf.decode(record.value, pKey).slice(1, 10)
            mentionsData.forEach((mention) => {
              if (
                mention.link &&
                typeof mention.link === 'string' &&
                (mention.link[0] === '@' || mention.link[0] === '%')
              ) {
                batch.push({
                  type: 'put',
                  key: [mention.link, 'm', shortKey],
                  value: processed,
                })
              }
            })
          }
        }
      }
    }

    return batch.length
  }

  function parseInt10(x) {
    return parseInt(x, 10)
  }

  function getResults(opts, live, cb) {
    pull(
      pl.read(level, opts),
      pull.collect((err, data) => {
        if (err) return cb(err)
        if (live) {
          const ps = pull(
            pl.read(level, Object.assign({}, opts, { live, old: false })),
            pull.map(parseInt10)
          )
          cb(null, or(seqs(data.map(parseInt10)), liveSeqs(ps)))
        } else cb(null, seqs(data.map(parseInt10)))
      })
    )
  }

  return {
    offset,
    stateLoaded,
    onData,
    writeBatch,

    name,
    remove: level.clear,
    close: level.close.bind(level),

    getMessagesByMention: function (key, live, cb) {
      getResults(
        {
          gte: [key, 'm', ''],
          lte: [key, 'm', undefined],
          keyEncoding: jsonCodec,
          keys: false,
        },
        live,
        cb
      )
    },
  }
}
