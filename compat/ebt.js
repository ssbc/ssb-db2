const pull = require('pull-stream')
const EBTIndex = require('../indexes/ebt')
const { onceWhen } = require('../utils')

class DebouncingBatchAdd {
  constructor(addBatch, period) {
    this.addBatch = addBatch
    this.period = period
    this.queueByAuthor = new Map()
    this.timestampsByAuthor = new Map()
    this.timer = null
  }

  flush(authorId) {
    const queue = this.queueByAuthor.get(authorId)
    const n = queue.length
    const msgVals = queue.map((x) => x[0])
    this.addBatch(msgVals, (err, kvts) => {
      if (err) {
        for (let i = 0; i < n; ++i) {
          const cb = queue[i][1]
          cb(err)
        }
      } else if (kvts.length !== n) {
        for (let i = 0; i < n; ++i) {
          const cb = queue[i][1]
          cb(new Error(`unexpected addBatch mismatch: ${kvts.length}} != ${n}`))
        }
      } else {
        for (let i = 0; i < n; ++i) {
          const kvt = kvts[i]
          const cb = queue[i][1]
          cb(null, kvt)
        }
      }
    })

    this.queueByAuthor.delete(authorId)
    this.timestampsByAuthor.delete(authorId)
  }

  scheduleFlush() {
    // Timer is already enabled
    if (this.timer) return

    this.timer = setInterval(() => {
      // Turn off the timer if there is nothing to flush
      if (this.queueByAuthor.size === 0) {
        clearInterval(this.timer)
        this.timer = null
      }
      // For each author, flush if enough time has passed
      else {
        const now = Date.now()
        for (const authorId of this.queueByAuthor.keys()) {
          const lastAdded = this.timestampsByAuthor.get(authorId)
          if (now - lastAdded > this.period) {
            this.flush(authorId)
          }
        }
      }
    }, this.period * 0.5)
  }

  add = (msgVal, cb) => {
    const authorId = msgVal.author
    const queue = this.queueByAuthor.get(authorId) || []
    queue.push([msgVal, cb])
    this.queueByAuthor.set(authorId, queue)
    this.timestampsByAuthor.set(authorId, Date.now())
    this.scheduleFlush()
  }
}

exports.init = function (sbot, config) {
  sbot.db.registerIndex(EBTIndex)
  if (!sbot.post) sbot.post = sbot.db.post
  sbot.getAtSequence = (key, cb) => {
    sbot.db.onDrain('ebt', () => {
      sbot.db.getIndex('ebt').getMessageFromAuthorSequence(key, cb)
    })
  }
  const debouncer = new DebouncingBatchAdd(
    sbot.db.addBatch,
    (config.db2 || {}).addBatchDebounce || 250
  )
  sbot.add = debouncer.add
  sbot.getVectorClock = function (cb) {
    onceWhen(
      sbot.db2migrate && sbot.db2migrate.synchronized,
      (isSynced) => isSynced,
      () => {
        sbot.db.onDrain('base', () => {
          const clock = {}
          pull(
            sbot.db.getAllLatest(),
            pull.through(({ key, value }) => {
              const authorId = key
              const { sequence } = value
              clock[authorId] = sequence
            }),
            pull.collect((err) => {
              if (err) return cb(err)
              cb(null, clock)
            })
          )
        })
      }
    )
  }
}
