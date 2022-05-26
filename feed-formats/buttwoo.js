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

const BUTTWOO_FEED_TF = bfe.toTF('feed', 'buttwoo-v1')

module.exports = function init(ssb) {
  const feedFormat = {
    name: 'buttwoo',
    encodings: ['js', 'bipf'],

    getFeedId(nativeMsg) {
      const pAuthor = 6 // we can assume this because the feed format is fixed
      const authorBFE = bipf.decode(nativeMsg, pAuthor)
      const pParent = pAuthor + bipf.encodingLength(authorBFE)
      const parentBFE = bipf.decode(nativeMsg, pParent)
      const [author, parent] = bfe.decode([authorBFE, parentBFE])
      if (parent) {
        const { data } = SSBURI.decompose(parent)
        return author + '/' + data
      } else {
        return author
      }
    },

    getMsgId(nativeMsg) {
      const [encodedValue, signature] = bipf.decode(nativeMsg)
      const data = blake3
        .hash(Buffer.concat([encodedValue, signature]))
        .toString('base64')
      return SSBURI.compose({ type: 'message', format: 'buttwoo-v1', data })
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
      const contentHash = blake3.hash(contentBuffer)
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
        const [encodedVal, sigBuf, contentBuffer] = bipf.decode(nativeMsg)
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
        const content = bipf.decode(contentBuffer)
        const contentHash = contentHashBuf
        const signature = sigBuf.toString('base64') + '.sig.ed25519'
        return {
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
      } else if (encoding === 'bipf') {
        const [encodedVal, sigBuf, contentBuffer] = bipf.decode(nativeMsg)
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
        bipf.markIdempotent(contentBuffer)
        const msgVal = {
          author,
          parent,
          sequence,
          timestamp,
          previous,
          content: contentBuffer,
          contentHash,
          signature,
          tag,
        }
        return bipf.allocAndEncode(msgVal)
      } else {
        // prettier-ignore
        throw new Error(`Feed format "${feedFormat.name}" does not support encoding "${encoding}"`)
      }
    },

    fromDecryptedNativeMsg(plaintextBuf, nativeMsg, encoding) {
      if (encoding !== 'js') {
        throw new Error('buttwoo only supports js encoding')
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
        const tag = Buffer.from([msgVal.tag])
        const contentBuffer = bipf.allocAndEncode(msgVal.content)
        const contentHash = blake3.hash(contentBuffer)
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
            throw new Error('Unknown field on buttwoo message: ' + key)
          }
          remaining.delete(key)
        })
        if (remaining.size > 0) {
          throw new Error('Missing fields on buttwoo message: ' + remaining)
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
      const [encodedValue, signature, contentBuffer] = bipf.decode(nativeMsg)
      const [
        authorBFE,
        parentBFE,
        sequence,
        timestamp,
        previousBFE,
        tag,
        contentSize,
        contentHash,
      ] = bipf.decode(encodedValue)

      if (contentHash.length !== 32)
        return cb(new Error('Content hash wrong size: ' + contentHash.length))

      if (!Buffer.isBuffer(tag))
        return cb(new Error('Tag is not a buffer: ' + tag))
      if (tag.length !== 1)
        return cb(new Error('Tag is not a single byte: ' + tag))
      const byte = tag[0]
      if (byte < 0 || byte > 2)
        return cb(new Error('Tag is not valid: ' + byte))

      if (contentBuffer.length !== contentSize)
        return cb(new Error('Content size does not match content'))

      const testedContentHash = blake3.hash(contentBuffer)
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

      if (!ssbKeys.verify(key, signature, hmacKey, encodedValue))
        return cb(new Error('Signature does not match encoded value'))

      // FIXME: check correct BFE types!
      // FIXME: check length of content

      if (prevNativeMsg !== null) {
        const [encodedValuePrev] = bipf.decode(prevNativeMsg)
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

        if (Buffer.compare(previousBFE, previousKeyBFE) !== 0)
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
