const bipf = require('bipf')
const Obv = require('obv')
const path = require('path')
const sort = require('ssb-sort')
const push = require('push-stream')
const Level = require('level')
const pl = require('pull-level')
const pull = require('pull-stream')
const debug = require('debug')

// 3 indexes:
// - root => msg seqs
// - mention => msg seqs
// - vote => msg seqs

module.exports = function (log, dir, feedId) {
  var seq = Obv()
  seq.set(-1)

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

  var batch = level.batch()
  const chunkSize = 512
  var messagesProcessed = 0

  function writeBatch() {
    batch.put(META, { version, seq: data.seq },
              { valueEncoding: 'json' }, throwOnError)
    batch.write()
  }

  function handleData(data) {
    var p = 0 // note you pass in p!
    p = bipf.seekKey(data.value, p, bKey)
    const key = bipf.decode(data.value, p)
    
    p = bipf.seekKey(data.value, p, bValue)
    if (~p) {
      // content
      var pContent = bipf.seekKey(data.value, p, bContent)
      if (~pContent) {
        var pRoot = bipf.seekKey(data.value, pContent, bRoot)
        if (~pRoot) {
          const root = bipf.decode(data.value, pRoot)
          if (root) {
            batch.put(['root', root, key], data.seq,
                      { keyEncoding: 'json' }, throwOnError)
          }
        }

        var pMentions = bipf.seekKey(data.value, pContent, bMentions)
        if (~pMentions) {
          const mentionsData = bipf.decode(data.value, pContent)
          if (Array.isArray(mentionsData)) {
            mentionsData.forEach(mention => {
              if (mention.link &&
                  typeof mention.link === 'string' &&
                  (mention.link[0] === '@' || mention.link[0] === '%')) {
                batch.put(['mention', mention.link, key], data.seq,
                          { keyEncoding: 'json' }, throwOnError)
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
                batch.put(['vlink', link, key], data.seq,
                          { keyEncoding: 'json' }, throwOnError)
              }
            }
          }
        }
      }
    }

    seq.set(data.seq)

    messagesProcessed++

    if (batch.length > chunkSize)
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
        
        debug(`social index scan time: ${Date.now()-start}ms, items: ${messagesProcessed}`)

        log.stream({ gt: seq.value, live: true }).pipe({
          paused: false,
          write: handleData
        })
      }
    })
  }
  
  level.get(META, (err, data) => {
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
        pl.read(level, { gte: '["mention", "' + key }),
        pull.collect((err, data) => {
          if (err) return cb(err)

          getMessagesFromSeqs(data.map(x => x.value), cb)
        })
      )
    },
    getMessagesByRoot: function(rootId, cb) {
      pull(
        pl.read(level, { gte: '["root", "' + rootId }),
        pull.collect((err, data) => {
          if (err) return cb(err)

          getMessagesFromSeqs(data.map(x => x.value), cb)
        })
      )
    },
    getMessagesByVoteLink: function(linkId, cb) {
      pull(
        pl.read(level, { gte: '["vlink", "' + linkId }),
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
