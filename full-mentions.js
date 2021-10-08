// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const FullMentionsIndex = require('./indexes/full-mentions')
const fullMentions = require('./operators/full-mentions')

exports.init = function (sbot, config) {
  sbot.db.registerIndex(FullMentionsIndex)
  sbot.db.operators.fullMentions = fullMentions
}
