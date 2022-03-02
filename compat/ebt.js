// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const pull = require('pull-stream')
const clarify = require('clarify-error')
const EBTIndex = require('../indexes/ebt')
const { onceWhen } = require('../utils')

exports.init = function (sbot, config) {
  sbot.db.registerIndex(EBTIndex)
  if (!sbot.post) sbot.post = sbot.db.post
  sbot.getAtSequence = (key, cb) => {
    sbot.db.onDrain('ebt', () => {
      sbot.db.getIndex('ebt').getMessageFromAuthorSequence(key, cb)
    })
  }
  sbot.add = sbot.db.add
  sbot.getVectorClock = function getVectorClock(cb) {
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
              if (err) return cb(clarify(err, 'ssb-db2 getVectorClock failed')) // prettier-ignore
              cb(null, clock)
            })
          )
        })
      }
    )
  }
}
