// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

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
  let i = 0
  let iTimer = 0
  let timer = null

  jitdb.status((jitStats) => {
    updateLog()
    statsObj.jit = jitStats
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
        obv.set(statsObj)
      }
    }, EMIT_INTERVAL)
    if (timer.unref) timer.unref()
  }

  function updateLog() {
    statsObj.log = log.since.value || 0
  }

  function updateIndex(name, offset) {
    updateLog()
    statsObj.indexes[name] = offset
    ++i
    if (!timer) {
      iTimer = i
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
