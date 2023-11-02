// SPDX-FileCopyrightText: 2022 Andre 'Staltz' Medeiros <contact@staltz.com>
//
// SPDX-License-Identifier: LGPL-3.0-only

const Obv = require('obz')

exports.init = function (sbot) {
  const post = Obv()
  sbot.db.post = post
  sbot.post = post
  sbot.db.onMsgAdded((ev) => {
    post.set(ev.kvt)
  })
}
