// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
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

module.exports = { onceWhen }
