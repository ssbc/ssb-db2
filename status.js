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
  const obv = Obv()
  obv.set(stats)
  const EMIT_INTERVAL = 1000
  let i = 0
  let iTimer = 0
  let timer = null

  function avgPercent(offsets, stats) {
    if (offsets.length === 0) return 1 // assume 100% if there are no numbers
    const logSize = Math.max(1, stats.log)
    const N = offsets.length
    return Math.min(
      offsets
        .map((offset) => Math.max(0, offset)) // avoid negative numbers
        .map((offset) => offset / logSize) // this index's progress
        .reduce((sum, x) => sum + x, 0) / N, // avg = (sum of all progress) / N
      1 // never go above 1
    )
  }

  function avgOffset(offsets, stats) {
    const logSize = Math.max(1, stats.log)
    return avgPercent(offsets, stats) * logSize
  }

  // Crunch stats numbers to produce one number for the "indexing" progress
  function calculateProgress() {
    const avgJITDBOffset = avgOffset(Object.values(stats.jit), stats)
    const offsets = Object.values(stats.indexes).concat(avgJITDBOffset)
    return avgPercent(offsets, stats)
  }

  jitdb.status((jitStats) => {
    updateLog()
    stats.jit = jitStats
    update()
  })

  function update() {
    ++i
    if (!timer) {
      iTimer = i
      emit()
      setTimer()
    }
  }

  function emit() {
    stats.progress = calculateProgress()
    obv.set(stats)
  }

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
        emit()
      }
    }, EMIT_INTERVAL)
    if (timer.unref) timer.unref()
  }

  function updateLog() {
    stats.log = log.since.value || 0
  }

  function updateIndex(name, offset) {
    if (stats.indexes[name] === offset) return
    updateLog()
    stats.indexes[name] = offset
    update()
  }

  return {
    obv,
    updateLog,
    updateIndex,
  }
}
