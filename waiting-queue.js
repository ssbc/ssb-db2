// log and seq are optional, if suppplied, get will make sure log is in sync before get cb
module.exports = function (log, seq) {
  var waiting = []
  var err, data, started = false

  function notifyWaiting()
  {
    if (!started) return

    const count = waiting.length

    if (count > 0 && started && seq != undefined ? seq.value === log.since.value : true) {
      for (var i = 0; i < count; ++i)
        waiting[i](err, data)

      waiting = waiting.slice(count)
    }
  }

  // setup handler for future
  if (seq != undefined)
    seq(notifyWaiting)

  return {
    done: function(errDone, dataDone) {
      err = errDone
      data = dataDone
      started = true

      notifyWaiting()
    },

    get: function(cb) {
      if (started && (seq != undefined ? seq.value === log.since.value : true))
        cb(err, data)
      else
        waiting.push(cb)
    },

    getFullySynced: function(cb) {
      log.onDrain(() => {
        if (started && (seq != undefined ? seq.value === log.since.value : true))
          cb(err, data)
        else
          waiting.push(cb)
      })
    }
  }
}
