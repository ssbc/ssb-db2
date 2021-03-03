/**
 * Obv utility to run the `cb` once, as soon as the condition given by
 * `filter` is true.
 */
function onceWhen(obv, filter, cb) {
  if (!obv) return cb()
  let answered = false
  let remove
  remove = obv((x) => {
    if (!filter(x)) return
    if (answered) {
      if (remove) remove(), (remove = null)
    } else {
      answered = true
      if (remove) remove(), (remove = null)
      cb()
    }
  })
}

module.exports = { onceWhen }
