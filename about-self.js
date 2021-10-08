// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const AboutSelfIndex = require('./indexes/about-self')

exports.init = function (sbot, config) {
  sbot.db.registerIndex(AboutSelfIndex)
}
