// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const pull = require('pull-stream')
const pullCont = require('pull-cont')
const ref = require('ssb-ref')
const Hookable = require('hoox')
const { author } = require('../operators')
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
    // default values
    const sequence = opts.sequence || opts.seq || 0
    const limit = opts.limit
    const keys = opts.keys === false ? false : true
    const values = opts.values === false ? false : true

    let query = author(opts.id)

    if (sequence) {
      query = {
        type: 'AND',
        data: [
          query,
          {
            type: 'GTE',
            data: {
              indexName: 'sequence',
              value: sequence,
            },
          },
        ],
      }
    }

    function formatMsg(msg) {
      msg = reEncrypt(msg)

      if (!keys && values) return msg.value
      else if (keys && !values) return msg.key
      else return msg
    }

    return pull(
      pullCont(function (cb) {
        sbot.db.getLog().onDrain(() => {
          if (!ref.isFeed(opts.id)) return cb(opts.id + ' is not a feed')

          if (limit) {
            sbot.db
              .getJITDB()
              .paginate(
                query,
                0,
                limit,
                false,
                false,
                'declared',
                (err, answer) => {
                  // prettier-ignore
                  if (err) cb(new Error('ssb-db2 createHistoryStream failed: ' + err.message))
                  else cb(null, pull.values(answer.results.map(formatMsg)))
                }
              )
          } else {
            sbot.db
              .getJITDB()
              .all(query, 0, false, false, 'declared', (err, results) => {
                // prettier-ignore
                if (err) cb(new Error('ssb-db2 createHistoryStream failed: ' + err.message))
                else cb(null, pull.values(results.map(formatMsg)))
              })
          }
        })
      })
    )
  })

  return {}
}
