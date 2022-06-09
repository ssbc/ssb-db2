// SPDX-FileCopyrightText: 2022 Andre 'Staltz' Medeiros
//
// SPDX-License-Identifier: LGPL-3.0-only

const bipf = require('bipf')
const bfe = require('ssb-bfe')
const blake3 = require('blake3')
const SSBURI = require('ssb-uri2')
const ssbKeys = require('ssb-keys')

function base64ToBuffer(str) {
  var i = str.indexOf('.')
  return Buffer.from(str.substring(0, i), 'base64')
}

function makeContentHash(contentBuffer) {
  return Buffer.concat([Buffer.from([0]), blake3.hash(contentBuffer)])
}

const BUTTWOO_FEED_TF = bfe.toTF('feed', 'buttwoo-v1')

module.exports = function init(ssb) {
  const feedFormat = {
    name: 'buttwoo-v1',
    encodings: ['js', 'bipf'],

    // FIXME: do these weakmaps make a difference? Should we keep or delete?
    // Which ones should we keep?
    _feedIdCache: new WeakMap(),
    _msgIdCache: new WeakMap(),
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

    getMsgId(nativeMsg) {
      if (feedFormat._msgIdCache.has(nativeMsg)) {
        return feedFormat._msgIdCache.get(nativeMsg)
      }
      const [encodedValue, signature] = feedFormat._extract(nativeMsg)
      const data = blake3
        .hash(Buffer.concat([encodedValue, signature]))
        .toString('base64')
      const msgId = SSBURI.compose({
        type: 'message',
        format: 'buttwoo-v1',
        data,
      })
      feedFormat._msgIdCache.set(nativeMsg, msgId)
      return msgId
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
        const signature = sigBuf.toString('base64') + '.sig.ed25519'
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
        const signature = sigBuf.toString('base64') + '.sig.ed25519'
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

    toNativeMsg(msgVal, encoding) {
      if (encoding === 'js') {
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
        const signature = base64ToBuffer(msgVal.signature)
        return bipf.allocAndEncode([encodedValue, signature, contentBuffer])
      } else if (encoding === 'bipf') {
        const remaining = new Set([
          'author',
          'parent',
          'sequence',
          'timestamp',
          'previous',
          'tag',
          'contentHash',
          'signature',
          'content',
        ])
        let authorBFE
        let parentBFE
        let sequence
        let timestamp
        let previousBFE
        let tag
        let contentHash
        let contentBuffer
        let signature
        bipf.iterate(msgVal, 0, (buf, valuePointer, keyPointer) => {
          const key = bipf.decode(buf, keyPointer)
          const val = key === 'content' ? null : bipf.decode(buf, valuePointer)
          if (key === 'author') {
            authorBFE = bfe.encode(val)
          } else if (key === 'parent') {
            parentBFE = bfe.encode(val)
          } else if (key === 'sequence') {
            sequence = val
          } else if (key === 'timestamp') {
            timestamp = val
          } else if (key === 'previous') {
            previousBFE = bfe.encode(val)
          } else if (key === 'tag') {
            tag = val
          } else if (key === 'contentHash') {
            contentHash = val
          } else if (key === 'signature') {
            signature = val
          } else if (key === 'content') {
            contentBuffer = bipf.pluck(buf, valuePointer)
          } else {
            throw new Error('Unknown field on buttwoo-v1 message: ' + key)
          }
          remaining.delete(key)
        })
        if (remaining.size > 0) {
          throw new Error('Missing fields on buttwoo-v1 message: ' + remaining)
        }
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
        const sigBuf = base64ToBuffer(signature)
        return bipf.allocAndEncode([encodedValue, sigBuf, contentBuffer])
      } else {
        // prettier-ignore
        throw new Error(`Feed format "${feedFormat.name}" does not support encoding "${encoding}"`)
      }
    },

    validateSingle: (hmacKey, nativeMsg, prevNativeMsg, cb) => {
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

      const author = bfe.decode(authorBFE)
      const { data: public } = SSBURI.decompose(author)
      const key = { public, curve: 'ed25519' }

      if (!ssbKeys.verify(key, sigBuf, hmacKey, encodedVal))
        return cb(new Error('Signature does not match encoded value'))

      // FIXME: check correct BFE types!
      // FIXME: check length of content

      if (prevNativeMsg !== null) {
        const prevMsgIdBFE = bfe.encode(feedFormat.getMsgId(prevNativeMsg))
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

  ssb.db.addFeedFormat(feedFormat)
}