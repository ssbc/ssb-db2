// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const pull = require('pull-stream')
const Hookable = require('hoox')
const EBTIndex = require('../indexes/ebt')
const { onceWhen } = require('../utils')

exports.init = function (sbot, config) {
  sbot.db.registerIndex(EBTIndex)
  if (!sbot.post) sbot.post = sbot.db.post
  sbot.getAtSequence = Hookable((key, cb) => {
    sbot.db.onDrain('ebt', () => {
      sbot.db.getIndex('ebt').getMessageFromAuthorSequence(key, cb)
    })
  })
  sbot.getAtSequenceNativeMsg = Hookable((key, feedFormat, cb) => {
    sbot.db.onDrain('ebt', () => {
      sbot.db
        .getIndex('ebt')
        .getNativeMsgFromAuthorSequence(key, feedFormat, cb)
    })
  })
  sbot.add = Hookable(sbot.db.add)
  sbot.getVectorClock = Hookable(function getVectorClock(cb) {
    onceWhen(
      sbot.db2migrate && sbot.db2migrate.synchronized,
      (isSynced) => isSynced === true,
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
            pull.onEnd((err) => {
              // prettier-ignore
              if (err) return cb(new Error('ssb-db2 getVectorClock failed', {cause: err}))
              cb(null, clock)
            })
          )
        })
      }
    )
  })
}
