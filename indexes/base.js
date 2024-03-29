// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const bipf = require('bipf')
const pl = require('pull-level')
const pull = require('pull-stream')
const Plugin = require('./plugin')

const BIPF_AUTHOR = bipf.allocAndEncode('author')
const BIPF_FEED = bipf.allocAndEncode('feed')
const BIPF_SEQUENCE = bipf.allocAndEncode('sequence')
const BIPF_OOO = bipf.allocAndEncode('ooo')

// feedId => latestMsg { offset, sequence }
//
// Necessary for feed validation and for EBT
module.exports = function makeBaseIndex(privateIndex) {
  return class BaseIndex extends Plugin {
    constructor(log, dir, configDb2) {
      super(log, dir, 'base', 2, undefined, 'json', configDb2)
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
            if (err && err !== true) cb(new Error('BaseIndex.onLoaded() failed', {cause: err}))
            else cb()
          }
        )
      )
    }

    processRecord(record, seq, pValue) {
      const buf = record.value

      // skip ooo messages, doesn't make sense for this index
      const pOOO = bipf.seekKey2(buf, 0, BIPF_OOO, 0)
      if (pOOO >= 0) return

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
          // prettier-ignore
          else cb(new Error('BaseIndex.removeFeedFromLatest() failed', {cause: err}))
          return
        }

        this.flush((err) => {
          // prettier-ignore
          if (err) cb(new Error('BaseIndex.removeFeedFromLatest() failed when waiting for flush', {cause: err}))
          else {
            this.level.del(feedId, (err2) => {
              // prettier-ignore
              if (err2) cb(new Error('BaseIndex.removeFeedFromLatest() failed when deleting', {cause: err2}))
              else cb()
            })
          }
        })
      })
    }
  }
}
