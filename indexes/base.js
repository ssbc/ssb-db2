// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const bipf = require('bipf')
const pl = require('pull-level')
const pull = require('pull-stream')
const clarify = require('clarify-error')
const Plugin = require('./plugin')

// authorId => latestMsg { offset, sequence }
//
// Necessary for feed validation and for EBT
module.exports = function makeBaseIndex(privateIndex) {
  return class BaseIndex extends Plugin {
    constructor(log, dir) {
      super(log, dir, 'base', 2, undefined, 'json')
      this.privateIndex = privateIndex
      this.authorLatest = new Map()
    }

    onLoaded(cb) {
      pull(
        this.getAllLatest(),
        pull.drain(
          ({ key, value }) => {
            this.authorLatest.set(key, value)
          },
          (err) => {
            if (err && err !== true) cb(clarify(err, 'BaseIndex.onLoaded() failed')) // prettier-ignore
            else cb()
          }
        )
      )
    }

    processRecord(record, seq) {
      const buf = record.value
      const pValue = bipf.seekKeyCached(buf, 0, 'value')
      if (pValue < 0) return
      const pValueAuthor = bipf.seekKeyCached(buf, pValue, 'author')
      if (pValueAuthor < 0) return
      const author = bipf.decode(buf, pValueAuthor)
      const pValueSequence = bipf.seekKeyCached(buf, pValue, 'sequence')
      if (pValueSequence < 0) return
      const sequence = bipf.decode(buf, pValueSequence)
      const latestSequence = this.authorLatest.has(author)
        ? this.authorLatest.get(author).sequence
        : 0
      if (sequence > latestSequence) {
        const latest = { offset: record.offset, sequence }
        this.authorLatest.set(author, latest)
        this.batch.push({
          type: 'put',
          key: author,
          value: latest,
        })
      }
    }

    indexesContent() {
      return false
    }

    onFlush(cb) {
      this.privateIndex.saveIndexes(cb)
    }

    // pull-stream where each item is { key, value }
    // where key is the authorId and value is { offset, sequence }
    getAllLatest() {
      const META = '\x00'
      return pl.read(this.level, {
        gt: META,
        valueEncoding: this.valueEncoding,
      })
    }

    // returns { offset, sequence }
    getLatest(feedId, cb) {
      this.level.get(feedId, { valueEncoding: this.valueEncoding }, cb)
    }

    removeFeedFromLatest(feedId, cb) {
      this.flush((err) => {
        if (err) cb(clarify(err, 'BaseIndex.removeFeedFromLatest() failed when waiting for flush')) // prettier-ignore
        else {
          this.level.del(feedId, (err2) => {
            if (err2) cb(clarify(err2, 'BaseIndex.removeFeedFromLatest() failed when deleting')) // prettier-ignore
            else cb()
          })
        }
      })
    }
  }
}
