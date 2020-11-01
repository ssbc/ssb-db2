const bipf = require('bipf')
const Obv = require('obv')
const path = require('path')
const sort = require('ssb-sort')
const push = require('push-stream')
const Level = require('level')
const pl = require('pull-level')
const pull = require('pull-stream')
const debug = require('debug')("social-index")

// 3 indexes:
// - root => msg seqs
// - mention => msg seqs
// - vote => msg seqs

module.exports = function (log, dir, feedId) {
  var seq = Obv()
  seq.set(-1)

  // FIXME: mkdirp

  var level = Level(path.join(dir, "indexes", "social"))
  const META = '\x00'
  const version = 1

  const bKey = Buffer.from('key')
  const bValue = Buffer.from('value')
  const bContent = Buffer.from('content')

  const bRoot = Buffer.from('root')
  const bMentions = Buffer.from('mentions')

  const bType = Buffer.from('type')
  const bVote = Buffer.from('vote')
  const bLink = Buffer.from('link')
  
  const throwOnError = function(err) { if (err) throw err }

  var batch = []
  const chunkSize = 512
  var isLive = false
  var processed = 0

  function writeBatch() {
    level.put(META, { version, seq: seq.value },
              { valueEncoding: 'json' }, throwOnError)

    level.batch(batch, { keyEncoding: 'json' }, throwOnError)
    batch = []
  }

  function handleData(data) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(data.value, p, bKey)
    const key = bipf.decode(data.value, p)

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
            batch.push({ type: 'put', key: ['r', root, key],
                         value: data.seq })
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
                batch.push({ type: 'put', key: ['m', mention.link, key],
                             value: data.seq })
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
                batch.push({ type: 'put', key: ['v', link, key],
                             value: data.seq })
              }
            }
          }
        }
      }
    }

    seq.set(data.seq)

    processed++

    if (batch.length > chunkSize || isLive)
      writeBatch()
  }
  
  function updateIndexes() {
    const start = Date.now()

    log.stream({ gt: seq.value }).pipe({
      paused: false,
      write: handleData,
      end: () => {
        if (batch.length > 0)
          writeBatch()
        
        debug(`social index scan time: ${Date.now()-start}ms, items: ${processed}`)

        isLive = true
        log.stream({ gt: seq.value, live: true }).pipe({
          paused: false,
          write: handleData
        })
      }
    })
  }
  
  level.get(META, { valueEncoding: 'json' }, (err, data) => {
    debug("got social index status:", data)

    if (data && data.version == version) {
      seq.set(data.seq)
      updateIndexes()
    } else
      level.clear(updateIndexes)
  })

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
  
  var self = {
    getMessagesByMention: function(key, cb) {
      pull(
        pl.read(level, {
          gte: ['m', key, ""],
          lte: ['m', key, undefined],
          keyEncoding: 'json'
        }),
        pull.collect((err, data) => {
          if (err) return cb(err)

          getMessagesFromSeqs(data.map(x => x.value), cb)
        })
      )
    },
    getMessagesByRoot: function(rootId, cb) {
      pull(
        pl.read(level, {
          gte: ['r', rootId, ""],
          lte: ['r', rootId, undefined],
          keyEncoding: 'json'
        }),
        pull.collect((err, data) => {
          if (err) return cb(err)

          getMessagesFromSeqs(data.map(x => x.value), cb)
        })
      )
    },
    getMessagesByVoteLink: function(linkId, cb) {
      pull(
        pl.read(level, {
          gte: ['v', linkId, ""],
          lte: ['v', linkId, undefined],
          keyEncoding: 'json'
        }),
        pull.collect((err, data) => {
          if (err) return cb(err)

          getMessagesFromSeqs(data.map(x => x.value), cb)
        })
      )
    },
    remove: level.clear
  }

  return self
}
