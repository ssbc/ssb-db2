const bipf = require('bipf')
const sort = require('ssb-sort')
const push = require('push-stream')
const pl = require('pull-level')
const pull = require('pull-stream')
const Plugin = require('./plugin')
const {query, fromDB, and, offsets} = require('../operators')

// 3 indexes:
// - root (msgId) => msg seqs
// - mentions (msgId) => msg seqs
// - votes (msgId) => msg seqs

module.exports = function (log, jitdb, dir, feedId) {

  const bKey = Buffer.from('key')
  const bValue = Buffer.from('value')
  const bContent = Buffer.from('content')

  const bRoot = Buffer.from('root')
  const bMentions = Buffer.from('mentions')

  const bType = Buffer.from('type')
  const bVote = Buffer.from('vote')
  const bLink = Buffer.from('link')

  var batch = []

  function writeData(cb) {
    level.batch(batch, { keyEncoding: 'json' }, cb)
    batch = []
  }

  function handleData(data, processed) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(data.value, p, bKey)
    const shortKey = bipf.decode(data.value, p).slice(1, 10)

    p = 0
    p = bipf.seekKey(data.value, p, bValue)
    if (~p) {
      // content
      var pContent = bipf.seekKey(data.value, p, bContent)
      if (~pContent) {
        var pRoot = bipf.seekKey(data.value, pContent, bRoot)
        if (~pRoot) {
          const root = bipf.decode(data.value, pRoot)
          if (root) {
            batch.push({ type: 'put', key: ['r', root, shortKey],
                         value: processed })
          }
        }

        var pMentions = bipf.seekKey(data.value, pContent, bMentions)
        if (~pMentions) {
          const mentionsData = bipf.decode(data.value, pMentions)
          if (Array.isArray(mentionsData)) {
            mentionsData.forEach(mention => {
              if (mention.link &&
                  typeof mention.link === 'string' &&
                  (mention.link[0] === '@' || mention.link[0] === '%')) {
                batch.push({ type: 'put', key: ['m', mention.link, shortKey],
                             value: processed })
              }
            })
          }
        }

        var pType = bipf.seekKey(data.value, pContent, bType)
        if (~pType) {
          if (bipf.compareString(data.value, pType, bVote) === 0) {
            var pVote = bipf.seekKey(data.value, pContent, bVote)
            if (~pVote) {
              var pLink = bipf.seekKey(data.value, pVote, bLink)
              if (~pLink) {
                const link = bipf.decode(data.value, pLink)
                batch.push({ type: 'put', key: ['v', link, shortKey],
                             value: processed })
              }
            }
          }
        }
      }
    }

    if (batch.length)
      return data.seq
    else
      return 0
  }

  function getMessagesFromSeqs(seqs, cb) {
    push(
      push.values(seqs),
      push.asyncMap(log.get),
      push.collect((err, results) => {
        const msgs = results.map(x => bipf.decode(x, 0))
        sort(msgs)
        msgs.reverse()
        cb(null, msgs)
      })
    )
  }

  const name = "social"
  let { level, seq } = Plugin(log, dir, name, 1, handleData, writeData)

  return {
    seq,
    name,
    remove: level.clear,
    getMessagesByMention: function(key, cb) {
      pull(
        pl.read(level, {
          gte: ['m', key, ""],
          lte: ['m', key, undefined],
          keyEncoding: 'json',
          keys: false
        }),
        pull.collect((err, data) => {
          if (err) return cb(err)

          cb(null, query(
            fromDB(jitdb),
            and(offsets(data.map(x => parseInt(x))))
          ))
        })
      )
    },
    getMessagesByRoot: function(rootId, cb) {
      pull(
        pl.read(level, {
          gte: ['r', rootId, ""],
          lte: ['r', rootId, undefined],
          keyEncoding: 'json',
          keys: false
        }),
        pull.collect((err, data) => {
          if (err) return cb(err)

          cb(null, query(
            fromDB(jitdb),
            and(offsets(data.map(x => parseInt(x))))
          ))
        })
      )
    },
    getMessagesByVoteLink: function(linkId, cb) {
      pull(
        pl.read(level, {
          gte: ['v', linkId, ""],
          lte: ['v', linkId, undefined],
          keyEncoding: 'json',
          keys: false
        }),
        pull.collect((err, data) => {
          if (err) return cb(err)

          cb(null, query(
            fromDB(jitdb),
            and(offsets(data.map(x => parseInt(x))))
          ))
        })
      )
    }
  }
}
