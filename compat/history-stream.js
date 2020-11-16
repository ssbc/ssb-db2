const pull = require('pull-stream')
const pullCont = require('pull-cont')
const ref = require('ssb-ref')
const { originalData } = require('../msg-utils')
const { author } = require("../operators")

// exports.name is blank to merge into global namespace

exports.manifest =  {
  createHistoryStream: 'source'
}

exports.permissions = {
  anonymous: {allow: ['createHistoryStream'], deny: null}
}

exports.init = function (sbot, config) {
  sbot.createHistoryStream = function(opts) {
    // default values
    const seq = opts.sequence || opts.seq || 0
    const limit = opts.limit
    const keys = opts.keys === false ? false : true
    const values = opts.values === false ? false : true

    let query = author(opts.id)

    if (seq) {
      query = {
        type: 'AND',
        data: [
          query,
          {
            type: 'GTE',
            data: {
              indexName: 'sequence',
              value: seq
            }
          }
        ]
      }
    }

    function formatMsg(msg) {
      const fixedMsg = originalData(msg)
      if (!keys && values)
        return fixedMsg.value
      else if (keys && !values)
        return fixedMsg.key
      else
        return fixedMsg
    }

    return pull(
      pullCont(function(cb) {
        if (!ref.isFeed(opts.id))
          return cb(opts.id + " is not a feed")

        if (limit) {
          sbot.db.jitdb.paginate(query, 0, limit, false, (err, results) => {
            cb(err, pull.values(results.data.map(x => formatMsg(x))))
          })
        } else {
          sbot.db.jitdb.all(query, 0, false, (err, results) => {
            cb(err, pull.values(results.map(x => formatMsg(x))))
          })
        }
      })
    )
  }

  return {}
}
