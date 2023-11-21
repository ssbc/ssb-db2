// SPDX-FileCopyrightText: 2022 Andre 'Staltz' Medeiros <contact@staltz.com>
//
// SPDX-License-Identifier: LGPL-3.0-only

const Hookable = require('hoox')

exports.init = function (sbot, config) {
  function publish(content, cb) {
    publishAs(config.keys, content, cb)
  }

  function publishAs(keys, content, cb) {
    sbot.db.create(
      {
        keys,
        content,
        feedFormat: 'classic',
        encoding: 'js',
      },
      cb
    )
  }

  sbot.db.publish = Hookable(publish)
  sbot.db.publishAs = Hookable(publishAs)
}
