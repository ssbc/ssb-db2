const bipf = require('bipf')
const pl = require('pull-level')
const pull = require('pull-stream')
const Plugin = require('./plugin')
const jsonCodec = require('flumecodec/json')
const { offsets, liveOffsets } = require('../operators')

// 3 indexes:
// - root (msgId) => msg seqs
// - mentions (msgId) => msg seqs
// - votes (msgId) => msg seqs

module.exports = function (log, dir) {
  const bKey = Buffer.from('key')
  const bValue = Buffer.from('value')
  const bContent = Buffer.from('content')

  const bRoot = Buffer.from('root')
  const bMentions = Buffer.from('mentions')

  const bType = Buffer.from('type')
  const bVote = Buffer.from('vote')
  const bLink = Buffer.from('link')

  let batch = []

  function writeData(cb) {
    level.batch(batch, { keyEncoding: jsonCodec }, cb)
    batch = []
  }

  function handleData(data, processed) {
    let p = 0 // note you pass in p!
    p = bipf.seekKey(data.value, p, bKey)
    const shortKey = bipf.decode(data.value, p).slice(1, 10)

    p = 0
    p = bipf.seekKey(data.value, p, bValue)
    if (~p) {
      // content
      const pContent = bipf.seekKey(data.value, p, bContent)
      if (~pContent) {
        const pRoot = bipf.seekKey(data.value, pContent, bRoot)
        if (~pRoot) {
          const root = bipf.decode(data.value, pRoot)
          if (root) {
            batch.push({
              type: 'put',
              key: ['r', root, shortKey],
              value: processed,
            })
          }
        }

        const pMentions = bipf.seekKey(data.value, pContent, bMentions)
        if (~pMentions) {
          const mentionsData = bipf.decode(data.value, pMentions)
          if (Array.isArray(mentionsData)) {
            mentionsData.forEach((mention) => {
              if (
                mention.link &&
                typeof mention.link === 'string' &&
                (mention.link[0] === '@' || mention.link[0] === '%')
              ) {
                batch.push({
                  type: 'put',
                  key: ['m', mention.link, shortKey],
                  value: processed,
                })
              }
            })
          }
        }

        const pType = bipf.seekKey(data.value, pContent, bType)
        if (~pType) {
          if (bipf.compareString(data.value, pType, bVote) === 0) {
            const pVote = bipf.seekKey(data.value, pContent, bVote)
            if (~pVote) {
              const pLink = bipf.seekKey(data.value, pVote, bLink)
              if (~pLink) {
                const link = bipf.decode(data.value, pLink)
                batch.push({
                  type: 'put',
                  key: ['v', link, shortKey],
                  value: processed,
                })
              }
            }
          }
        }
      }
    }

    if (batch.length) return batch.length
    else return 0
  }

  function parseInt10(x) {
    return parseInt(x, 10)
  }

  const name = 'social'
  const { level, seq } = Plugin(log, dir, name, 1, handleData, writeData)

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
          cb(null, liveOffsets(data.map(parseInt10), ps))
        } else cb(null, offsets(data.map(parseInt10)))
      })
    )
  }

  return {
    seq,
    name,
    remove: level.clear,
    close: level.close.bind(level),
    getMessagesByMention: function (key, live, cb) {
      getResults(
        {
          gte: ['m', key, ''],
          lte: ['m', key, undefined],
          keyEncoding: jsonCodec,
          keys: false,
        },
        live,
        cb
      )
    },
    getMessagesByRoot: function (rootId, live, cb) {
      getResults(
        {
          gte: ['r', rootId, ''],
          lte: ['r', rootId, undefined],
          keyEncoding: jsonCodec,
          keys: false,
        },
        live,
        cb
      )
    },
    getMessagesByVoteLink: function (linkId, live, cb) {
      getResults(
        {
          gte: ['v', linkId, ''],
          lte: ['v', linkId, undefined],
          keyEncoding: jsonCodec,
          keys: false,
        },
        live,
        cb
      )
    },
  }
}
