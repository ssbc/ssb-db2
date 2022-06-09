// SPDX-FileCopyrightText: 2022 Andre 'Staltz' Medeiros
//
// SPDX-License-Identifier: LGPL-3.0-only

const Ref = require('ssb-ref')
const bipf = require('bipf')
const validate2 = require('ssb-validate2')
const ssbKeys = require('ssb-keys')

module.exports = function init(ssb) {
  const feedFormat = {
    name: 'classic',
    encodings: ['js', 'bipf'],

    getFeedId(nativeMsg) {
      return nativeMsg.author
    },

    getMsgId(nativeMsg) {
      return '%' + ssbKeys.hash(JSON.stringify(nativeMsg, null, 2))
    },

    isNativeMsg(x) {
      return typeof x === 'object' && !!x && Ref.isFeedId(x.author)
    },

    isAuthor(author) {
      return Ref.isFeedId(author)
    },

    toPlaintextBuffer(opts) {
      return Buffer.from(JSON.stringify(opts.content), 'utf8')
    },

    newNativeMsg(opts) {
      // FIXME: validate opts.previous.key
      // FIXME: validate opts.previous.value.sequence
      // FIXME: validate opts.keys.id
      // FIXME: validate opts.timestamp
      const previous = opts.previous || { key: null, value: { sequence: 0 } }
      const nativeMsg = {
        previous: previous.key,
        sequence: previous.value.sequence + 1,
        author: opts.keys.id,
        timestamp: +opts.timestamp,
        hash: 'sha256',
        content: opts.content,
      }
      // var err = isInvalidShape(msg)
      // if (err) throw err
      // FIXME: make it a best practice that newNativeMsg should be valid and
      // does not require validation after this point
      return ssbKeys.signObj(opts.keys, opts.hmacKey, nativeMsg)
    },

    fromNativeMsg(nativeMsg, encoding) {
      if (encoding === 'js') {
        // FIXME: set WeakMap for msgVal => nativeMsg
        return nativeMsg
      } else if (encoding === 'bipf') {
        return bipf.encoding(nativeMsg)
      } else {
        // prettier-ignore
        throw new Error(`Feed format "${feedFormat.name}" does not support encoding "${encoding}"`)
      }
    },

    fromDecryptedNativeMsg(plaintextBuf, nativeMsg, encoding) {
      if (encoding === 'js') {
        const msgVal = nativeMsg
        const content = JSON.parse(plaintextBuf.toString('utf8'))
        msgVal.content = content
        return msgVal
      } else if (encoding === 'bipf') {
        // TODO: some kind of bipf.replace() API
        throw new Error('Not yet implemented')
      } else {
        // prettier-ignore
        throw new Error(`Feed format "${feedFormat.name}" does not support encoding "${encoding}"`)
      }
    },

    toNativeMsg(serialized, encoding) {
      if (encoding === 'js') {
        return serialized
      } else if (encoding === 'bipf') {
        return bipf.decode(serialized)
      } else {
        // prettier-ignore
        throw new Error(`Feed format "${feedFormat.name}" does not support encoding "${encoding}"`)
      }
    },

    validateBatch: validate2.validateBatch,
    validateOOOBatch: validate2.validateOOOBatch,
    validateSingle: validate2.validateSingle,
  }

  ssb.db.installFeedFormat(feedFormat)
}
