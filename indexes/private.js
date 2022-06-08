// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const Obv = require('obz')
const bipf = require('bipf')
const clarify = require('clarify-error')
const DeferredPromise = require('p-defer')
const path = require('path')
const Debug = require('debug')
const NumsFile = require('../nums-file')

const { indexesPath } = require('../defaults')

module.exports = function (dir, sbot, config) {
  const latestOffset = Obv()
  const stateLoaded = DeferredPromise()

  const startDecryptBox1 = config.db2.startDecryptBox1
    ? new Date(config.db2.startDecryptBox1)
    : null

  const debug = Debug('ssb:db2:private')

  // FIXME: BAD NAME!!! actually means "encrypted messages that I cant decrypt NOW but maybe one day I can"
  const encryptedIdx = new NumsFile(
    path.join(indexesPath(dir), 'encrypted.index')
  )
  // an option is to cache the read keys instead of only where the
  // messages are, this has an overhead around storage.  The
  // performance of that is a decrease in unbox time to 50% of
  // original for box1 and around 75% box2
  // FIXME: rename to 'decrypted'?
  const canDecryptIdx = new NumsFile(
    path.join(indexesPath(dir), 'canDecrypt.index')
  )

  function loadIndexes(cb) {
    encryptedIdx.loadFile((err) => {
      if (err) {
        debug('failed to load encrypted')
        latestOffset.set(-1)
        // FIXME: wait for all encryptionFormats ready
        // if (sbot.box2) sbot.box2.isReady(stateLoaded.resolve)
        //else
        stateLoaded.resolve()
        if (err.code === 'ENOENT') cb()
        else if (err.message === 'Empty NumsFile') cb()
        // prettier-ignore
        else cb(clarify(err, 'private plugin failed to load "encrypted" index'))
        return
      }
      debug('encrypted loaded', encryptedIdx.size())

      canDecryptIdx.loadFile((err) => {
        latestOffset.set(Math.min(encryptedIdx.offset, canDecryptIdx.offset))
        // FIXME:
        // if (sbot.box2) sbot.box2.isReady(stateLoaded.resolve)
        //else
        stateLoaded.resolve()
        debug('loaded offset', latestOffset.value)

        cb()
      })
    })
  }

  loadIndexes((err) => {
    if (err) throw err
  })

  let savedTimer
  function saveIndexes(cb) {
    if (!savedTimer) {
      savedTimer = setTimeout(() => {
        savedTimer = null
        encryptedIdx.saveFile(latestOffset.value)
        canDecryptIdx.saveFile(latestOffset.value)
      }, 1000)
    }
    cb()
  }

  const BIPF_VALUE = bipf.allocAndEncode('value')
  const BIPF_CONTENT = bipf.allocAndEncode('content')
  const BIPF_AUTHOR = bipf.allocAndEncode('author')
  const BIPF_PREVIOUS = bipf.allocAndEncode('previous')
  const BIPF_TIMESTAMP = bipf.allocAndEncode('timestamp')

  function ciphertextStrToBuffer(str) {
    const dot = str.indexOf('.')
    return Buffer.from(str.slice(0, dot), 'base64')
  }

  function decryptAndReconstruct(ciphertext, record, pValue) {
    const recBuffer = record.value

    // Get encryption format
    const encryptionFormat = sbot.db.findEncryptionFormatFor(ciphertext)
    if (!encryptionFormat) return null

    // Get previous
    const pPrevious = bipf.seekKey2(recBuffer, pValue, BIPF_PREVIOUS, 0)
    if (pPrevious < 0) return null
    const previous = bipf.decode(recBuffer, pPrevious)

    // Get author
    const pAuthor = bipf.seekKey2(recBuffer, pValue, BIPF_AUTHOR, 0)
    if (pAuthor < 0) return null
    const author = bipf.decode(recBuffer, pAuthor)

    // Get feed format
    const feedFormat = sbot.db.findFeedFormatForAuthor(author)
    if (!feedFormat) return null

    // Decrypt
    const ciphertextBuf = ciphertextStrToBuffer(ciphertext)
    const opts = { keys: config.keys, author, previous }
    const plaintextBuf = encryptionFormat.decrypt(ciphertextBuf, opts)
    if (!plaintextBuf) return null

    // Reconstruct KVT in JS encoding
    const kvt = bipf.decode(recBuffer, 0)
    const originalContent = kvt.value.content
    const nativeMsg = feedFormat.toNativeMsg(kvt.value, 'js')
    const msgVal = feedFormat.fromDecryptedNativeMsg(
      plaintextBuf,
      nativeMsg,
      'js'
    )
    kvt.value = msgVal
    kvt.meta = {
      private: true,
      originalContent,
    }

    // Encode it back to BIPF
    const newRecBuffer = bipf.allocAndEncode(kvt)
    return { offset: record.offset, value: newRecBuffer }
  }

  function decrypt(record, streaming) {
    const recOffset = record.offset
    const recBuffer = record.value
    if (!recBuffer) return record
    if (canDecryptIdx.has(recOffset)) {
      const pValue = bipf.seekKey2(recBuffer, 0, BIPF_VALUE, 0)
      if (pValue < 0) return record
      const pContent = bipf.seekKey2(recBuffer, pValue, BIPF_CONTENT, 0)
      if (pContent < 0) return record

      const ciphertext = bipf.decode(recBuffer, pContent)
      const originalMsg = decryptAndReconstruct(ciphertext, record, pValue)
      if (!originalMsg) return record

      return originalMsg
    } else if (recOffset > latestOffset.value || !streaming) {
      if (streaming) latestOffset.set(recOffset)

      const pValue = bipf.seekKey2(recBuffer, 0, BIPF_VALUE, 0)
      if (pValue < 0) return record
      const pContent = bipf.seekKey2(recBuffer, pValue, BIPF_CONTENT, 0)
      if (pContent < 0) return record

      const type = bipf.getEncodedType(recBuffer, pContent)
      if (type !== bipf.types.string) return record

      const ciphertext = bipf.decode(recBuffer, pContent)

      // FIXME: This block, doing box1 and box2 things, is "SPECIAL" logic
      // WHERE DO WE PUT IT?
      if (ciphertext.endsWith('.box') && startDecryptBox1) {
        // FIXME: should this be a special config coming from encryptionFormat?
        const pTimestamp = bipf.seekKey2(recBuffer, pValue, BIPF_TIMESTAMP, 0)
        const declaredTimestamp = bipf.decode(recBuffer, pTimestamp)
        if (declaredTimestamp < startDecryptBox1) return record
      }
      if (streaming && ciphertext.endsWith('.box2'))
        encryptedIdx.insert(recOffset)

      const originalMsg = decryptAndReconstruct(ciphertext, record, pValue)
      if (!originalMsg) return record

      canDecryptIdx.insert(recOffset)

      if (!streaming) saveIndexes(() => {})
      return originalMsg
    } else {
      return record
    }
  }

  function missingDecrypt() {
    return encryptedIdx.filterOut(canDecryptIdx)
  }

  function reset(cb) {
    encryptedIdx.reset()
    canDecryptIdx.reset()
    latestOffset.set(-1)
    saveIndexes(cb)
  }

  return {
    latestOffset,
    decrypt,
    missingDecrypt,
    saveIndexes,
    reset,
    stateLoaded: stateLoaded.promise,
  }
}

module.exports.reEncrypt = function (msg) {
  if (msg.meta && msg.meta.private) {
    msg.value.content = msg.meta.originalContent
    delete msg.meta
  }
  return msg
}
