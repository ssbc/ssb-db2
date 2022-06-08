// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

module.exports = [
  require('./core'),
  require('./feed-formats/classic'),
  require('./feed-formats/bendybutt-v1'),
  require('./feed-formats/buttwoo-v1'), // FIXME: remove this from index.js
  require('./encryption-formats/box1'),
  require('./encryption-formats/box2'),
  require('./compat/publish'),
  require('./migrate'),
]
