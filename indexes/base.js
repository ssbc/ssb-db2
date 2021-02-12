const bipf = require('bipf')
const pl = require('pull-level')
const pull = require('pull-stream')
const Plugin = require('./plugin')

// 1 index:
// - author => latest { msg key, sequence timestamp } (validate state & EBT)

const bKey = Buffer.from('key')
const bValue = Buffer.from('value')
const bAuthor = Buffer.from('author')
const bSequence = Buffer.from('sequence')
const bTimestamp = Buffer.from('timestamp')

module.exports = class BaseIndex extends Plugin {
  constructor(log, dir, privateIndex) {
    super(dir, 'base', 1, undefined, 'json')
    this.privateIndex = privateIndex
    this.authorLatest = {}
  }

  flushBatch(cb) {
    super.flushBatch((err) => {
      if (err) return cb(err)
      else this.privateIndex.saveIndexes(cb)
    })
  }

  handleRecord(record, seq) {
    if (record.offset < this.offset.value) return
    const buf = record.value
    if (!buf) return // deleted

    const pValue = bipf.seekKey(buf, 0, bValue)
    if (pValue >= 0) {
      const author = bipf.decode(buf, bipf.seekKey(buf, pValue, bAuthor))
      const sequence = bipf.decode(buf, bipf.seekKey(buf, pValue, bSequence))
      const timestamp = bipf.decode(buf, bipf.seekKey(buf, pValue, bTimestamp))

      let latestSequence = 0
      if (this.authorLatest[author])
        latestSequence = this.authorLatest[author].sequence
      if (sequence > latestSequence) {
        const key = bipf.decode(buf, bipf.seekKey(buf, 0, bKey))
        this.authorLatest[author] = { id: key, sequence, timestamp }
        this.batch.push({
          type: 'put',
          key: author,
          value: this.authorLatest[author],
        })
      }
    }
    return
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

  beforeIndexUpdate(cb) {
    this.getAllLatest((err, latest) => {
      this.authorLatest = latest
      cb()
    })
  }

  // returns { id (msg key), sequence, timestamp }
  getLatest(feedId, cb) {
    this.level.get(feedId, { valueEncoding: this.valueEncoding }, cb)
  }

  removeFeedFromLatest(feedId) {
    this.level.del(feedId, (err) => {
      if (err) throw err
    })
  }
}
