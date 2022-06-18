// SPDX-FileCopyrightText: 2022 Andre 'Staltz' Medeiros
//
// SPDX-License-Identifier: LGPL-3.0-only

const sodium = require('chloride')
const privateBox = require('private-box')
const Ref = require('ssb-ref')
const {
  isButtwooV1FeedSSBURI,
  isBendyButtV1FeedSSBURI,
  decompose,
  isFeedSSBURI,
} = require('ssb-uri2')

module.exports = function init(ssb) {
  const NAME = 'box1'

  const encryptionFormat = {
    name: NAME,
    suffix: 'box',

    encrypt(plaintextBuf, opts) {
      const encryptionKeys = opts.recps
        .map(function convertToBase64DataStr(recp) {
          if (Ref.isFeed(recp)) return recp.slice(1, -8)
          else if (
            isFeedSSBURI(recp) ||
            isBendyButtV1FeedSSBURI(recp) ||
            isButtwooV1FeedSSBURI(recp)
          )
            return decompose(recp).data
          else if (recp && typeof recp === 'string')
            throw new Error('box1 does not support recipient ' + recp)
          else return null
        })
        .filter((maybeBase64DataStr) => !!maybeBase64DataStr)
        .map((base64DataStr) => Buffer.from(base64DataStr, 'base64'))
        .map(sodium.crypto_sign_ed25519_pk_to_curve25519)

      return privateBox.multibox(plaintextBuf, encryptionKeys)
    },

    decrypt(ciphertextBuf, opts) {
      const secretKey =
        opts.keys._exchangeKey || // use the cache
        sodium.crypto_sign_ed25519_sk_to_curve25519(
          Buffer.from(opts.keys.private, 'base64')
        )
      if (opts.keys.private) opts.keys._exchangeKey = secretKey // set the cache
      return privateBox.multibox_open(ciphertextBuf, secretKey)
    },
  }

  if (ssb.db) ssb.db.installEncryptionFormat(encryptionFormat)
}
