// SPDX-FileCopyrightText: 2022 Andre 'Staltz' Medeiros
//
// SPDX-License-Identifier: LGPL-3.0-only

const bipf = require('bipf')
const bfe = require('ssb-bfe')
const blake3 = require('blake3')
const SSBURI = require('ssb-uri2')
const varint = require('fast-varint')
const ssbKeys = require('ssb-keys')

function base64ToBuffer(str) {
  var i = str.indexOf('.')
  return Buffer.from(str.substring(0, i), 'base64')
}

function makeContentHash(contentBuffer) {
  return Buffer.concat([Buffer.from([0]), blake3.hash(contentBuffer)])
}

const BUTTWOO_FEED_TF = bfe.toTF('feed', 'buttwoo-v1')
const BUTTWOO_MSG_TF = bfe.toTF('message', 'buttwoo-v1')
const BIPF_TAG_SIZE = 3
const BIPF_TAG_MASK = 7
const BIPF_STRING_TYPE = 0b000

module.exports = function init(ssb) {
  const feedFormat = {
    name: 'buttwoo-v1',
    encodings: ['js', 'bipf'],

    _feedIdCache: new WeakMap(),
    _msgIdCache: new WeakMap(),
    _msgIdStringCache: new WeakMap(),
    _msgIdBFECache: new WeakMap(),
    _jsMsgValCache: new WeakMap(),
    _bipfMsgValCache: new WeakMap(),
    _extractCache: new WeakMap(),

    _extract(nativeMsg) {
      if (feedFormat._extractCache.has(nativeMsg)) {
        return feedFormat._extractCache.get(nativeMsg)
      }
      const arr = bipf.decode(nativeMsg)
      feedFormat._extractCache.set(nativeMsg, arr)
      return arr
    },

    getFeedId(nativeMsg) {
      if (feedFormat._feedIdCache.has(nativeMsg)) {
        return feedFormat._feedIdCache.get(nativeMsg)
      }
      const [encodedValue] = feedFormat._extract(nativeMsg)
      let authorBFE
      let parentBFE
      bipf.iterate(encodedValue, 0, (b, pointer) => {
        if (!authorBFE) {
          authorBFE = bipf.decode(b, pointer)
        } else if (!parentBFE) {
          parentBFE = bipf.decode(b, pointer)
          return true // abort the bipf.iterate
        }
      })
      const author = bfe.decode(authorBFE)
      const parent = bfe.decode(parentBFE)
      if (parent) {
        const { data } = SSBURI.decompose(parent)
        const feedId = author + '/' + data
        feedFormat._feedIdCache.set(nativeMsg, feedId)
        return author + '/' + data
      } else {
        feedFormat._feedIdCache.set(nativeMsg, author)
        return author
      }
    },

    getMsgIdHelper(nativeMsg) {
      let data = feedFormat._msgIdCache.get(nativeMsg)
      if (!data) {
        const [encodedValue, signature] = feedFormat._extract(nativeMsg)
        data = blake3
          .hash(Buffer.concat([encodedValue, signature]))
        feedFormat._msgIdCache.set(nativeMsg, data)
      }
      return data
    },

    getMsgId(nativeMsg) {
      if (feedFormat._msgIdStringCache.has(nativeMsg)) {
        return feedFormat._msgIdStringCache.get(nativeMsg)
      }

      let data = feedFormat.getMsgIdHelper(nativeMsg)

      // Fast:
      const msgId = `ssb:message/buttwoo-v1/${data.toString('base64')
        .replace(/\+/g, '-')
        .replace(/\//g, '_')}`
      // Proper:
      // const msgId = SSBURI.compose({
      //   type: 'message',
      //   format: 'buttwoo-v1',
      //   data,
      // })
      feedFormat._msgIdStringCache.set(nativeMsg, msgId)
      return msgId
    },

    _getMsgIdBFE(nativeMsg) {
      if (feedFormat._msgIdBFECache.has(nativeMsg)) {
        return feedFormat._msgIdBFECache.get(nativeMsg)
      }

      let data = feedFormat.getMsgIdHelper(nativeMsg)
      const msgIdBFE = Buffer.concat([BUTTWOO_MSG_TF, data])
      feedFormat._msgIdBFECache.set(nativeMsg, msgIdBFE)
      return msgIdBFE
    },

    getSequence(nativeMsg) {
      const [encodedVal] = feedFormat._extract(nativeMsg)
      let sequence
      let i = 0
      bipf.iterate(encodedVal, 0, (b, pointer) => {
        if (i++ === 2) {
          sequence = bipf.decode(b, pointer)
          return true // abort the bipf.iterate
        }
      })
      return sequence
    },

    isNativeMsg(x) {
      if (!Buffer.isBuffer(x)) return false
      if (x.length === 0) return false
      const type = bipf.getEncodedType(x)
      if (type !== bipf.types.array) return false
      // Peek into the BFE header of the author field
      const bfeHeader = x.slice(8, 10)
      return bfeHeader.compare(BUTTWOO_FEED_TF) === 0
    },

    isAuthor(author) {
      // FIXME: ssb-uri2 needs to be updated so that it supports both
      // ssb:feed/buttwoo-v1/$AUTHOR and ssb:feed/buttwoo-v1/$AUTHOR/$PARENT
      // Then we can delete the right-hand side of the OR.
      return (
        SSBURI.isButtwooV1FeedSSBURI(author) ||
        author.startsWith('ssb:feed/buttwoo-v1/')
      )
    },

    toPlaintextBuffer(opts) {
      return bipf.allocAndEncode(opts.content)
    },

    newNativeMsg(opts) {
      // FIXME: validate ALL the stuff
      const authorBFE = bfe.encode(opts.keys.id)
      const previous = opts.previous || { key: null, value: { sequence: 0 } }
      const previousBFE = bfe.encode(previous.key)
      const contentBuffer = bipf.allocAndEncode(opts.content)
      const contentHash = makeContentHash(contentBuffer)
      const parentBFE = bfe.encode(opts.parent || null)
      const tag = Buffer.from([opts.tag])
      const sequence = previous.value.sequence + 1
      const timestamp = +opts.timestamp

      const value = [
        authorBFE,
        parentBFE,
        sequence,
        timestamp,
        previousBFE,
        tag,
        contentBuffer.length,
        contentHash,
      ]

      const encodedValue = bipf.allocAndEncode(value)
      // FIXME: we need ssb-keys to support returning buffer from sign()
      const signature = ssbKeys.sign(opts.keys, opts.hmacKey, encodedValue)
      const sigBuf = base64ToBuffer(signature)

      return bipf.allocAndEncode([encodedValue, sigBuf, contentBuffer])
    },

    fromNativeMsg(nativeMsg, encoding) {
      if (encoding === 'js') {
        if (feedFormat._jsMsgValCache.has(nativeMsg)) {
          return feedFormat._jsMsgValCache.get(nativeMsg)
        }
        const [encodedVal, sigBuf, contentBuf] = feedFormat._extract(nativeMsg)
        const [
          authorBFE,
          parentBFE,
          sequence,
          timestamp,
          previousBFE,
          tag,
          contentLength,
          contentHashBuf,
        ] = bipf.decode(encodedVal)
        const author = bfe.decode(authorBFE)
        const parent = bfe.decode(parentBFE)
        const previous = bfe.decode(previousBFE)
        const content = bipf.decode(contentBuf)
        const contentHash = contentHashBuf
        const signature = sigBuf
        const msgVal = {
          author,
          parent,
          sequence,
          timestamp,
          previous,
          tag,
          content,
          contentHash,
          signature,
        }
        feedFormat._jsMsgValCache.set(nativeMsg, msgVal)
        return msgVal
      } else if (encoding === 'bipf') {
        if (feedFormat._bipfMsgValCache.has(nativeMsg)) {
          return feedFormat._bipfMsgValCache.get(nativeMsg)
        }
        const [encodedVal, sigBuf, contentBuf] = feedFormat._extract(nativeMsg)
        const [
          authorBFE,
          parentBFE,
          sequence,
          timestamp,
          previousBFE,
          tag,
          contentLength,
          contentHash,
        ] = bipf.decode(encodedVal)
        const author = bfe.decode(authorBFE)
        const parent = bfe.decode(parentBFE)
        const previous = bfe.decode(previousBFE)
        const signature = sigBuf
        bipf.markIdempotent(contentBuf)
        const msgVal = {
          author,
          parent,
          sequence,
          timestamp,
          previous,
          content: contentBuf,
          contentHash,
          signature,
          tag,
        }
        const bipfMsg = bipf.allocAndEncode(msgVal)
        feedFormat._bipfMsgValCache.set(nativeMsg, bipfMsg)
        return bipfMsg
      } else {
        // prettier-ignore
        throw new Error(`Feed format "${feedFormat.name}" does not support encoding "${encoding}"`)
      }
    },

    fromDecryptedNativeMsg(plaintextBuf, nativeMsg, encoding) {
      if (encoding !== 'js') {
        throw new Error('buttwoo-v1 only supports js encoding when decrypting')
      }
      const msgVal = feedFormat.fromNativeMsg(nativeMsg, encoding)
      const content = bipf.decode(plaintextBuf)
      msgVal.content = content
      return msgVal
    },

    _toNativeMsgJS(msgVal) {
      const authorBFE = bfe.encode(msgVal.author)
      const parentBFE = bfe.encode(msgVal.parent)
      const sequence = msgVal.sequence
      const timestamp = msgVal.timestamp
      const previousBFE = bfe.encode(msgVal.previous)
      const tag = msgVal.tag
      const contentBuffer = bipf.allocAndEncode(msgVal.content)
      const contentHash = msgVal.contentHash
      const value = [
        authorBFE,
        parentBFE,
        sequence,
        timestamp,
        previousBFE,
        tag,
        contentBuffer.length,
        contentHash,
      ]
      const encodedValue = bipf.allocAndEncode(value)
      const signature = msgVal.signature
      return bipf.allocAndEncode([encodedValue, signature, contentBuffer])
    },

    _toNativeMsgBIPF(buffer) {
      let authorBFE, parentBFE, sequence, timestamp, previousBFE
      let tagBuffer, contentBuffer, contentLen, contentHash, sigBuf

      const tag = varint.decode(buffer, 0)
      const len = tag >> BIPF_TAG_SIZE

      for (var c = varint.decode.bytes; c < len; ) {
        const keyStart = c
        var keyTag = varint.decode(buffer, keyStart)
        c += varint.decode.bytes
        c += keyTag >> BIPF_TAG_SIZE
        const valueStart = c
        const valueTag = varint.decode(buffer, valueStart)
        const valueLen = varint.decode.bytes + (valueTag >> BIPF_TAG_SIZE)

        const key = bipf.decode(buffer, keyStart)
        if (key === 'author')
          authorBFE = bfe.encode(bipf.decode(buffer, valueStart))
        else if (key === 'parent')
          parentBFE = bfe.encode(bipf.decode(buffer, valueStart))
        else if (key === 'sequence') sequence = bipf.decode(buffer, valueStart)
        else if (key === 'timestamp')
          timestamp = bipf.decode(buffer, valueStart)
        else if (key === 'previous')
          previousBFE = bfe.encode(bipf.decode(buffer, valueStart))
        else if (key === 'tag') tagBuffer = bipf.decode(buffer, valueStart)
        else if (key === 'content') {
          if ((valueTag & BIPF_TAG_MASK) === BIPF_STRING_TYPE) {
            contentBuffer = bipf.decode(buffer, valueStart)
            contentLen = base64ToBuffer(contentBuffer).length
          } else {
            contentBuffer = bipf.pluck(buffer, valueStart)
            contentLen = contentBuffer.length
          }
        } else if (key === 'contentHash')
          contentHash = bipf.decode(buffer, valueStart)
        else if (key === 'signature') sigBuf = bipf.decode(buffer, valueStart)

        c += valueLen
      }

      const value = [
        authorBFE,
        parentBFE,
        sequence,
        timestamp,
        previousBFE,
        tagBuffer,
        contentLen,
        contentHash,
      ]
      const encodedValue = bipf.allocAndEncode(value)
      return bipf.allocAndEncode([encodedValue, sigBuf, contentBuffer])
    },

    toNativeMsg(msgVal, encoding) {
      if (encoding === 'js') {
        return feedFormat._toNativeMsgJS(msgVal)
      } else if (encoding === 'bipf') {
        return feedFormat._toNativeMsgBIPF(msgVal)
      } else {
        // prettier-ignore
        throw new Error(`Feed format "${feedFormat.name}" does not support encoding "${encoding}"`)
      }
    },

    validate(nativeMsg, prevNativeMsg, hmacKey, cb) {
      const [encodedVal, sigBuf, contentBuf] = feedFormat._extract(nativeMsg)
      const [
        authorBFE,
        parentBFE,
        sequence,
        timestamp,
        previousBFE,
        tag,
        contentSize,
        contentHash,
      ] = bipf.decode(encodedVal)

      if (contentHash.length !== 33)
        return cb(new Error('Content hash wrong size: ' + contentHash.length))

      if (!Buffer.isBuffer(tag))
        return cb(new Error('Tag is not a buffer: ' + tag))
      if (tag.length !== 1)
        return cb(new Error('Tag is not a single byte: ' + tag))
      const byte = tag[0]
      if (byte < 0 || byte > 2)
        return cb(new Error('Tag is not valid: ' + byte))

      if (contentBuf.length !== contentSize)
        return cb(new Error('Content size does not match content'))

      const testedContentHash = makeContentHash(contentBuf)

      if (Buffer.compare(testedContentHash, contentHash) !== 0)
        return cb(new Error('Content hash does not match content'))

      if (
        typeof timestamp !== 'number' ||
        isNaN(timestamp) ||
        !isFinite(timestamp)
      )
        return cb(
          new Error(
            `invalid message: timestamp is "${timestamp}", expected a JavaScript number`
          )
        )

      // Fast:
      const public = authorBFE.slice(2)
      // Proper:
      // const { data: public } = SSBURI.decompose(bfe.decode(authorBFE))

      const keys = { public, curve: 'ed25519' }

      if (!ssbKeys.verify(keys, sigBuf, hmacKey, encodedVal))
        return cb(new Error('Signature does not match encoded value'))

      // FIXME: check correct BFE types!
      // FIXME: check length of content

      if (prevNativeMsg !== null) {
        const prevMsgIdBFE = feedFormat._getMsgIdBFE(prevNativeMsg)
        const [encodedValuePrev] = feedFormat._extract(prevNativeMsg)
        const [
          authorBFEPrev,
          parentBFEPrev,
          sequencePrev,
          timestampPrev,
          previousBFEPrev,
          tagPrev,
        ] = bipf.decode(encodedValuePrev)

        if (Buffer.compare(authorBFE, authorBFEPrev) !== 0)
          return cb(new Error('Author does not match previous message'))

        if (Buffer.compare(parentBFE, parentBFEPrev) !== 0)
          return cb(new Error('Parent does not match previous message'))

        if (sequence !== sequencePrev + 1)
          return cb(new Error('Sequence must increase'))

        if (timestamp <= timestampPrev)
          return cb(new Error('Timestamp must increase'))

        if (Buffer.compare(previousBFE, prevMsgIdBFE) !== 0)
          return cb(
            new Error('Previous does not match key of previous message')
          )

        if (tagPrev[0] === 2) return cb(new Error('Feed already terminated'))
      } else {
        if (sequence !== 1)
          return cb(new Error('Sequence must be 1 for first message'))

        if (!bfe.isEncodedGenericNil(previousBFE))
          return cb(new Error('Previous must be nil for first message'))
      }

      cb()
    },
  }

  ssb.db.installFeedFormat(feedFormat)
}
