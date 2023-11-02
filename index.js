// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

module.exports = [
  require('./core'),
  require('ssb-classic'),
  require('ssb-box'),
  require('ssb-box2'),
  require('./compat/publish'),
  require('./migrate'),
]
