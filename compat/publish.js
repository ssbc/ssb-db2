// SPDX-FileCopyrightText: 2022 Andre 'Staltz' Medeiros <contact@staltz.com>
//
// SPDX-License-Identifier: LGPL-3.0-only

const ssbKeys = require('ssb-keys')
const validate = require('ssb-validate')
const Ref = require('ssb-ref')
const { onceWhen } = require('../utils')

exports.init = function (sbot, config) {
  const hmacKey = null

  function guardAgainstDuplicateLogs(methodName) {
    if (sbot.db2migrate && sbot.db2migrate.doesOldLogExist()) {
      return new Error(
        'ssb-db2: refusing to ' +
          methodName +
          ' because the old log still exists. ' +
          'This is to protect your feed from forking ' +
          'into an irrecoverable state.'
      )
    }
  }

  function encryptContent(keys, content) {
    if (
      sbot.box2 &&
      sbot.box2.supportsBox2 &&
      content.recps.every(sbot.box2.supportsBox2)
    ) {
      const latestMsg = sbot.db.getState().getAsKV(keys.id)
      return sbot.box2.encryptClassic(
        keys,
        content,
        latestMsg ? latestMsg.key : null
      )
    } else return ssbKeys.box(content, content.recps)
  }

  function publish(content, cb) {
    const guard = guardAgainstDuplicateLogs('publish()')
    if (guard) return cb(guard)

    publishAs(config.keys, content, cb)
  }

  function publishAs(keys, content, cb) {
    const guard = guardAgainstDuplicateLogs('publishAs()')
    if (guard) return cb(guard)

    if (!Ref.isFeedId(keys.id)) {
      // prettier-ignore
      return cb(new Error('publishAs() does not support feed format ' + keys.id))
    }

    onceWhen(
      sbot.db.stateFeedsReady,
      (ready) => ready === true,
      () => {
        if (content.recps) {
          try {
            content = encryptContent(keys, content)
          } catch (ex) {
            return cb(ex)
          }
        }
        const latestKVT = sbot.db.getState().getAsKV(keys.id)
        const msgVal = validate.create(
          latestKVT ? { queue: [latestKVT] } : null,
          keys,
          hmacKey,
          content,
          Date.now()
        )
        sbot.db.addImmediately(msgVal, cb)
      }
    )
  }

  sbot.db.publish = publish
  sbot.db.publishAs = publishAs
}
