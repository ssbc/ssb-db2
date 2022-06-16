// SPDX-FileCopyrightText: 2022 Andre 'Staltz' Medeiros
//
// SPDX-License-Identifier: LGPL-3.0-only

const BFE = require('ssb-bfe')
const bendyButt = require('ssb-bendy-butt')
const bencode = require('bencode')
const SSBURI = require('ssb-uri2')
const ssbKeys = require('ssb-keys')

const CONTENT_SIG_PREFIX = Buffer.from('bendybutt', 'utf8')

module.exports = function init(ssb) {
  const feedFormat = {
    name: 'bendybutt-v1',
    encodings: ['js'],

    getFeedId(nativeMsg) {
      return BFE.decode(bencode.decode(nativeMsg, 2, 39))
    },

    getMsgId(nativeMsg) {
      let data = ssbKeys.hash(nativeMsg)
      if (data.endsWith('.sha256')) data = data.slice(0, -'.sha256'.length)
      return SSBURI.compose({ type: 'message', format: 'bendybutt-v1', data })
    },

    getSequence(nativeMsg) {
      const msgVal = feedFormat.fromNativeMsg(nativeMsg, 'js')
      return msgVal.sequence
    },

    isNativeMsg(x) {
      if (!Buffer.isBuffer(x)) return false
      if (x.length === 0) return false
      try {
        const author = BFE.decode(bencode.decode(x, 2, 39))
        return SSBURI.isBendyButtV1FeedSSBURI(author)
      } catch (err) {
        return false
      }
    },

    isAuthor(author) {
      return SSBURI.isBendyButtV1FeedSSBURI(author)
    },

    toPlaintextBuffer(opts) {
      const { content, contentKeys, keys, hmacKey } = opts
      const contentBFE = BFE.encode(content)
      const contentSignature = ssbKeys.sign(
        contentKeys || keys,
        hmacKey,
        Buffer.concat([CONTENT_SIG_PREFIX, bencode.encode(contentBFE)])
      )
      const contentSection = [content, contentSignature]
      return bencode.encode(BFE.encode(contentSection))
    },

    newNativeMsg(opts) {
      // FIXME: validate opts.previous.key
      // FIXME: validate opts.previous.value.sequence
      // FIXME: validate opts.keys.id
      // FIXME: validate opts.timestamp
      const author = opts.keys.id
      const previous = opts.previous || { key: null, value: { sequence: 0 } }
      const sequence = previous.value.sequence + 1
      const previousId = previous.key
      const timestamp = +opts.timestamp
      const content = opts.content
      const contentBFE = BFE.encode(content)
      const contentSignature = ssbKeys.sign(
        opts.contentKeys || opts.keys,
        opts.hmacKey,
        Buffer.concat([CONTENT_SIG_PREFIX, bencode.encode(contentBFE)])
      )
      let contentSection = [content, contentSignature]
      const payload = [author, sequence, previousId, timestamp, contentSection]
      const signature = ssbKeys.sign(
        opts.keys,
        opts.hmacKey,
        bencode.encode(BFE.encode(payload))
      )
      return bencode.encode(BFE.encode([payload, signature]))
    },

    fromNativeMsg(nativeMsg, encoding) {
      if (encoding === 'js') {
        // FIXME: set WeakMap for msgVal => nativeMsg
        return bendyButt.decode(nativeMsg)
      } else {
        // prettier-ignore
        throw new Error(`Feed format "${feedFormat.name}" does not support encoding "${encoding}"`)
      }
    },

    fromDecryptedNativeMsg(plaintextBuf, nativeMsg, encoding) {
      if (encoding === 'js') {
        const msgVal = feedFormat.fromNativeMsg(nativeMsg, encoding)
        const contentSection = BFE.decode(bencode.decode(plaintextBuf))
        const [content, contentSignature] = contentSection
        msgVal.content = content
        msgVal.contentSignature = contentSignature
        return msgVal
      } else {
        // prettier-ignore
        throw new Error(`Feed format "${feedFormat.name}" does not support encoding "${encoding}"`)
      }
    },

    toNativeMsg(msg, encoding) {
      if (encoding === 'js') {
        return bendyButt.encode(msg)
      } else {
        // prettier-ignore
        throw new Error(`Feed format "${feedFormat.name}" does not support encoding "${encoding}"`)
      }
    },

    validate(nativeMsg, previousNativeMsg, hmacKey, cb) {
      const msgVal = feedFormat.fromNativeMsg(nativeMsg, 'js')
      const previous = previousNativeMsg
        ? feedFormat.fromNativeMsg(previousNativeMsg, 'js')
        : null
      const err = bendyButt.validateSingle(msgVal, previous, hmacKey)
      if (err) return cb(err)
      cb(null)
    },
  }

  ssb.db.installFeedFormat(feedFormat)
}
