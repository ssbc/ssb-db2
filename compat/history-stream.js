// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const pull = require('pull-stream')
const ref = require('ssb-ref')
const Hookable = require('hoox')
const { author, and, gte, where, batch, live, toPullStream } = require('../operators')
const { reEncrypt } = require('../indexes/private')

// exports.name is blank to merge into global namespace

exports.manifest = {
  createHistoryStream: 'source',
}

exports.permissions = {
  anonymous: { allow: ['createHistoryStream'], deny: null },
}

exports.init = function (sbot, config) {
  sbot.createHistoryStream = Hookable(function createHistoryStream(opts) {
    if (!ref.isFeed(opts.id)) {
      return pull.error(Error(opts.id + ' is not a feed'))
    }
    // default values
    const sequence = opts.sequence || opts.seq || 0
    const limit = opts.limit
    const keys = opts.keys === false ? false : true
    const values = opts.values === false ? false : true

    let query = author(opts.id)
    if (sequence) {
      query = and(query, gte(sequence, 'sequence'))
    }

    function formatMsg(msg) {
      msg = reEncrypt(msg)

      if (!keys && values) return msg.value
      else if (keys && !values) return msg.key
      else return msg
    }

    return pull(
      sbot.db.query(
        where(query),
        limit ? batch(limit) : null,
        opts.live ? live({ old: true }) : null,
        toPullStream()
      ),
      limit ? pull.take(limit) : null,
      pull.map(formatMsg)
    )
  })

  return {}
}
