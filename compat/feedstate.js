// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const { onceWhen } = require('../utils')

exports.init = function (sbot, config) {
  sbot.getFeedState = function (feedId, cb) {
    onceWhen(
      sbot.db.stateFeedsReady,
      (ready) => ready === true,
      () => {
        const latest = sbot.db.getState().getAsKV(feedId)

        // this covers the case where you have a brand new feed
        if (!latest) return cb(null, { id: null, sequence: 0 })

        return cb(null, {
          id: latest.key,
          sequence: latest.value.sequence,
        })
      }
    )
  }
}
