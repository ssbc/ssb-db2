// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const Obv = require('obz')

module.exports = function Status(log, jitdb) {
  const stats = {
    log: log.since.value || 0,
    jit: {},
    indexes: {},
    progress: 0,
  }
  let prevProgress = 0
  const obv = Obv()
  obv.set(stats)
  const EMIT_INTERVAL = 1000
  let i = 0
  let iTimer = 0
  let timer = null

  // Crunch stats numbers to produce one number for the "indexing" progress
  function calculateProgress() {
    const logSize = Math.max(1, stats.log) // 1 prevents division by zero
    const nums = Object.values(stats.indexes).concat(Object.values(stats.jit))
    const N = Math.max(1, nums.length) // 1 prevents division by zero
    stats.progress = Math.min(
      nums
        .map((offset) => Math.max(0, offset)) // avoid -1 numbers
        .map((offset) => offset / logSize) // this index's progress
        .reduce((acc, x) => acc + x, 0) / N, // avg = (sum of all progress) / N
      1 // never go above 1
    )
    const justFinished = stats.progress === 1 && prevProgress < 1
    prevProgress = stats.progress
    if (justFinished) jitdb.resetStatus()
  }

  jitdb.status((jitStats) => {
    updateLog()
    stats.jit = jitStats
    calculateProgress()
    obv.set(stats)
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
        calculateProgress()
        obv.set(stats)
      }
    }, EMIT_INTERVAL)
    if (timer.unref) timer.unref()
  }

  function updateLog() {
    stats.log = log.since.value || 0
  }

  function updateIndex(name, offset) {
    updateLog()
    stats.indexes[name] = offset
    ++i
    if (!timer) {
      iTimer = i
      calculateProgress()
      obv.set(stats)
      setTimer()
    }
  }

  return {
    obv,
    updateLog,
    updateIndex,
  }
}
