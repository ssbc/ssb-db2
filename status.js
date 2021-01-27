const Obv = require('obz')

module.exports = function Status(log, jitdb) {
  const statsObj = {
    log: log.since.value || 0,
    jit: {},
    indexes: {},
  }
  const obv = Obv()
  obv.set(statsObj)
  const EMIT_INTERVAL = 1000
  const PRUNE_INTERVAL = 2000
  let jitdbLastTime = Date.now()
  let i = 0
  let iTimer = 0
  let timer = null

  jitdb.status((jitStats) => {
    updateLog()
    statsObj.jit = jitStats
    jitdbLastTime = Date.now()
    obv.set(statsObj)
  })

  function setTimer() {
    // Turn on
    timer = setInterval(() => {
      if (i === iTimer) {
        // Turn off because nothing has been updated recently
        clearInterval(timer)
        timer = null
        i = iTimer = 0
      } else {
        iTimer = i
        if (jitdbLastTime + PRUNE_INTERVAL < Date.now()) {
          statsObj.jit = {}
        }
        obv.set(statsObj)
      }
    }, EMIT_INTERVAL)
    if (timer.unref) timer.unref()
  }

  function updateLog() {
    statsObj.log = log.since.value
  }

  function updateIndex(name, offset) {
    updateLog()
    statsObj.indexes[name] = offset
    ++i
    if (!timer) {
      iTimer = i
      if (jitdbLastTime + PRUNE_INTERVAL < Date.now()) {
        statsObj.jit = {}
      }
      obv.set(statsObj)
      setTimer()
    }
  }

  return {
    obv,
    updateLog,
    updateIndex,
  }
}
