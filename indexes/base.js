// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const bipf = require('bipf')
const pl = require('pull-level')
const pull = require('pull-stream')
const clarify = require('clarify-error')
const Plugin = require('./plugin')

const BIPF_AUTHOR = bipf.allocAndEncode('author')
const BIPF_FEED = bipf.allocAndEncode('feed')
const BIPF_SEQUENCE = bipf.allocAndEncode('sequence')

// feedId => latestMsg { offset, sequence }
//
// Necessary for feed validation and for EBT
module.exports = function makeBaseIndex(privateIndex) {
  return class BaseIndex extends Plugin {
    constructor(log, dir) {
      super(log, dir, 'base', 2, undefined, 'json')
      this.privateIndex = privateIndex
      this.feedLatest = new Map()
    }

    onLoaded(cb) {
      pull(
        this.getAllLatest(),
        pull.drain(
          ({ key, value }) => {
            this.feedLatest.set(key, value)
          },
          (err) => {
            // prettier-ignore
            if (err && err !== true) cb(clarify(err, 'BaseIndex.onLoaded() failed'))
            else cb()
          }
        )
      )
    }

    processRecord(record, seq, pValue) {
      const buf = record.value
      const pValueAuthor = bipf.seekKey2(buf, pValue, BIPF_AUTHOR, 0)
      const pValueSequence = bipf.seekKey2(buf, pValue, BIPF_SEQUENCE, 0)
      const author = bipf.decode(buf, pValueAuthor)
      const pFeed = bipf.seekKey2(buf, 0, BIPF_FEED, 0)
      const feedId = pFeed < 0 ? author : bipf.decode(buf, pFeed)
      const sequence = bipf.decode(buf, pValueSequence)
      const latestSequence = this.feedLatest.has(feedId)
        ? this.feedLatest.get(feedId).sequence
        : 0
      if (sequence > latestSequence) {
        const latest = { offset: record.offset, sequence }
        this.feedLatest.set(feedId, latest)
        this.batch.push({
          type: 'put',
          key: feedId,
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

    reset() {
      this.feedLatest.clear()
    }

    // pull-stream where each item is { key, value }
    // where key is the feedId and value is { offset, sequence }
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
      this.getLatest(feedId, (err) => {
        if (err) {
          if (err.name === 'NotFoundError') cb()
          else cb(clarify(err, 'BaseIndex.removeFeedFromLatest() failed'))
          return
        }

        this.flush((err) => {
          // prettier-ignore
          if (err) cb(clarify(err, 'BaseIndex.removeFeedFromLatest() failed when waiting for flush'))
          else {
            this.level.del(feedId, (err2) => {
              // prettier-ignore
              if (err2) cb(clarify(err2, 'BaseIndex.removeFeedFromLatest() failed when deleting'))
              else cb()
            })
          }
        })
      })
    }
  }
}
