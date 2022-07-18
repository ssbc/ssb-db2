// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

module.exports = class DebouncingBatchAdd {
  constructor(addBatch, period) {
    this.addBatch = addBatch
    this.period = period
    this.queueByFeed = new Map()
    this.timestampsByFeed = new Map()
    this.timer = null
  }

  flush(feedId) {
    const queue = this.queueByFeed.get(feedId)
    const n = queue.length
    const msgVals = queue.map((x) => x[0])
    // Clear the queue memory BEFORE the callbacks trigger more queue additions
    this.queueByFeed.delete(feedId)
    this.timestampsByFeed.delete(feedId)
    // Add the messages in the queue
    const [msgVal1, opts1, cb1] = queue[0]
    this.addBatch(msgVals, opts1, (err, kvts) => {
      if (err) {
        for (let i = 0; i < n; ++i) {
          const cb = queue[i][2]
          cb(err)
        }
      } else if (kvts.length !== n) {
        for (let i = 0; i < n; ++i) {
          const cb = queue[i][2]
          cb(new Error(`unexpected addBatch mismatch: ${kvts.length}} != ${n}`))
        }
      } else {
        for (let i = 0; i < n; ++i) {
          const kvt = kvts[i]
          const cb = queue[i][2]
          cb(null, kvt)
        }
      }
    })
  }

  scheduleFlush() {
    // Timer is already enabled
    if (this.timer) return

    this.timer = setInterval(() => {
      // Turn off the timer if there is nothing to flush
      if (this.queueByFeed.size === 0) {
        clearInterval(this.timer)
        this.timer = null
      }
      // For each feed, flush if enough time has passed
      else {
        const now = Date.now()
        for (const feedId of this.queueByFeed.keys()) {
          const lastAdded = this.timestampsByFeed.get(feedId)
          if (now - lastAdded > this.period) {
            this.flush(feedId)
          }
        }
      }
    }, this.period * 0.5)
  }

  add(msgVal, opts, cb) {
    const feedId = opts.feedId
    const queue = this.queueByFeed.get(feedId) || []
    queue.push([msgVal, opts, cb])
    this.queueByFeed.set(feedId, queue)
    this.timestampsByFeed.set(feedId, Date.now())
    this.scheduleFlush()
  }
}
