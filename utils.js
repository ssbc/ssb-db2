// SPDX-FileCopyrightText: 2021-2022 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

/**
 * Obv utility to run the `cb` once, as soon as the condition given by
 * `filter` is true.
 */
function onceWhen(obv, filter, cb) {
  if (!obv) return cb()
  let answered = false
  let remove
  remove = obv((x) => {
    if (answered) return
    if (!filter(x)) return

    answered = true
    cb()

    if (!remove) return
    setTimeout(() => {
      if (!remove) return
      remove()
      remove = null
    })
  })
}

function onceWhenPromise(obv, filter) {
  return new Promise((resolve) => {
    onceWhen(obv, filter, resolve)
  })
}

class ReadyGate {
  constructor() {
    this.waiting = new Set()
    this.ready = false
  }

  onReady(cb) {
    if (this.ready) cb()
    else this.waiting.add(cb)
  }

  setReady() {
    this.ready = true
    for (const cb of this.waiting) cb()
    this.waiting.clear()
  }
}

module.exports = { onceWhen, onceWhenPromise, ReadyGate }
