// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

// DEPRECATED
// please use https://github.com/ssbc/ssb-about-self
const AboutSelfIndex = require('./indexes/about-self')

exports.init = function (sbot, config) {
  console.warn('ssb-db2/about-self is deprecated, please use ssb-about-self')

  sbot.db.registerIndex(AboutSelfIndex)
}
