// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const { deferred } = require('jitdb/operators')

module.exports = function mentions(key) {
  return deferred((meta, cb) => {
    meta.db.onDrain('fullMentions', () => {
      meta.db.getIndex('fullMentions').getMessagesByMention(key, meta.live, cb)
    })
  })
}
