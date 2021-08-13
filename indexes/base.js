const bipf = require('bipf')
const pl = require('pull-level')
const pull = require('pull-stream')
const Plugin = require('./plugin')

const B_KEY = Buffer.from('key')
const B_VALUE = Buffer.from('value')
const B_AUTHOR = Buffer.from('author')
const B_SEQUENCE = Buffer.from('sequence')
const B_TIMESTAMP = Buffer.from('timestamp')

// author => latest { msg key, sequence timestamp } (validate state & EBT)
module.exports = function makeBaseIndex(privateIndex) {
  return class BaseIndex extends Plugin {
    constructor(log, dir) {
      super(log, dir, 'base', 1, undefined, 'json')
      this.privateIndex = privateIndex
      this.authorLatest = {}
    }

    onLoaded(cb) {
      this.getAllLatest((err, latest) => {
        this.authorLatest = latest
        cb()
      })
    }

    processRecord(record, seq) {
      const buf = record.value
      const pValue = bipf.seekKey(buf, 0, B_VALUE)
      if (pValue < 0) return
      const author = bipf.decode(buf, bipf.seekKey(buf, pValue, B_AUTHOR))
      const sequence = bipf.decode(buf, bipf.seekKey(buf, pValue, B_SEQUENCE))
      const timestamp = bipf.decode(buf, bipf.seekKey(buf, pValue, B_TIMESTAMP))
      let latestSequence = 0
      if (this.authorLatest[author])
        latestSequence = this.authorLatest[author].sequence
      if (sequence > latestSequence) {
        const key = bipf.decode(buf, bipf.seekKey(buf, 0, B_KEY))
        this.authorLatest[author] = { id: key, sequence, timestamp }
        this.batch.push({
          type: 'put',
          key: author,
          value: this.authorLatest[author],
        })
      }
    }

    indexesContent() {
      return false
    }

    onFlush(cb) {
      this.privateIndex.saveIndexes(cb)
    }

    getAllLatest(cb) {
      const META = '\x00'
      pull(
        pl.read(this.level, {
          gt: META,
          valueEncoding: this.valueEncoding,
        }),
        pull.collect((err, data) => {
          if (err) return cb(err)
          const result = {}
          data.forEach((d) => {
            result[d.key] = d.value
          })
          cb(null, result)
        })
      )
    }

    // returns { id (msg key), sequence, timestamp }
    getLatest(feedId, cb) {
      this.level.get(feedId, { valueEncoding: this.valueEncoding }, cb)
    }

    removeFeedFromLatest(feedId, cb) {
      this.flush((err) => {
        if (err) cb(err)
        else
          this.level.del(feedId, (err2) => {
            if (err2) cb(err2)
            else cb()
          })
      })
    }
  }
}
