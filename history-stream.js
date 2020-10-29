const pull = require('pull-stream')
const pullCont = require('pull-cont')
const { originalData } = require('./msg-utils')

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
    const limit = opts.limit || 1e10
    const keys = opts.keys === false ? false : true
    const values = opts.values === false ? false : true

    const query = {
      type: 'EQUAL',
      data: {
        seek: sbot.db.jitdb.seekAuthor,
        value: opts.id,
        indexType: "author",
        indexAll: true
      }
    }

    function formatMsg(msg) {
      let fixedMsg = originalData(msg)
      if (!keys && values)
        return fixedMsg.value
      else if (keys && !values)
        return fixedMsg.key
      else
        return fixedMsg
    }

    return pull(
      pullCont(function(cb) {
        const partialState = sbot.db.partial.getSync()
        if (seq && partialState[opts.id] && partialState[opts.id].full === true) {
          // sequences starts with 1, offset starts with 0 ;-)
          sbot.db.jitdb.query(query, seq - 1, limit, true, (err, results) => {
            cb(err, pull.values(results.map(x => formatMsg(x))))
          })
        }
        else if (seq) {
          // for partial feeds we don't know where seq is compared to
          // and offset, so we lean on the fact that we probably don't
          // have a lot of messages for this feed.

          sbot.db.jitdb.query(query, (err, results) => {
            if (err) return cb(err)
            pull(
              pull.values(results),
              pull.filter(x => x.value.sequence >= seq),
              pull.take(limit),
              pull.collect((err, filtered) => {
                cb(null, pull.values(filtered.map(x => formatMsg(x))))
              })
            )
          })
        }
        else
          sbot.db.jitdb.query(query, (err, results) => {
            cb(err, pull.values(results.map(x => formatMsg(x))))
          })
      })
    )
  }

  return {}
}
