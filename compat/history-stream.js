const pull = require('pull-stream')
const pullCont = require('pull-cont')
const ref = require('ssb-ref')
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
  sbot.createHistoryStream = function (opts) {
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
              .paginate(query, 0, limit, false, false, (err, answer) => {
                cb(err, pull.values(answer.results.map(formatMsg)))
              })
          } else {
            sbot.db.getJITDB().all(query, 0, false, false, (err, results) => {
              cb(err, pull.values(results.map(formatMsg)))
            })
          }
        })
      })
    )
  }

  return {}
}
