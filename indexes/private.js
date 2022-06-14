// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const Obv = require('obz')
const bipf = require('bipf')
const clarify = require('clarify-error')
const DeferredPromise = require('p-defer')
const path = require('path')
const Debug = require('debug')
const multicb = require('multicb')
const NumsFile = require('../nums-file')
const { indexesPath } = require('../defaults')

module.exports = function (dir, sbot, config) {
  const latestOffset = Obv()
  const stateLoaded = DeferredPromise()

  const startDecryptBox1 = config.db2.startDecryptBox1
    ? new Date(config.db2.startDecryptBox1)
    : null

  const debug = Debug('ssb:db2:private')

  function pathFor(filename) {
    return path.join(indexesPath(dir), filename)
  }

  let decryptedIdx
  let encryptedIdxMap

  function loadIndexes(cb) {
    const done = multicb({ pluck: 1 })

    decryptedIdx = new NumsFile(pathFor('decrypted.index'))

    encryptedIdxMap = new Map()
    for (const encryptionFormat of sbot.db.encryptionFormats) {
      encryptedIdxMap.set(
        encryptionFormat.name,
        new NumsFile(pathFor(`encrypted-${encryptionFormat.name}.index`))
      )
      encryptionFormat.onReady(done())
    }

    decryptedIdx.loadFile(done())
    for (const idx of encryptedIdxMap.values()) {
      idx.loadFile(done())
    }

    done((err) => {
      if (err) {
        debug('failed to load encrypted or decrypted indexes')
        latestOffset.set(-1)
        stateLoaded.resolve()
        if (err.code === 'ENOENT') cb()
        else if (err.message === 'Empty NumsFile') cb()
        // prettier-ignore
        else cb(clarify(err, 'private plugin failed to load'))
        return
      }

      debug('decrypted loaded, size: ' + decryptedIdx.size())
      for (const [name, idx] of encryptedIdxMap) {
        debug(`encrypted-${name} loaded, size: ${idx.size()}`)
      }

      const encryptedIdxOffsets = [...encryptedIdxMap.values()].map(
        (idx) => idx.offset
      )
      latestOffset.set(Math.min(decryptedIdx.offset, ...encryptedIdxOffsets))
      stateLoaded.resolve()
      debug('loaded offset', latestOffset.value)
      cb()
    })
  }

  // Wait for secret-stack plugins (which may add encryption formats) to load
  setTimeout(() => {
    loadIndexes((err) => {
      if (err) throw err
    })
  })

  let savedTimer
  function saveIndexes(cb) {
    if (!savedTimer) {
      savedTimer = setTimeout(() => {
        savedTimer = null
        decryptedIdx.saveFile(latestOffset.value)
        for (const idx of encryptedIdxMap.values()) {
          idx.saveFile(latestOffset.value)
        }
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
      encryptionFormat: encryptionFormat.name,
    }

    // Encode it back to BIPF
    const newRecBuffer = bipf.allocAndEncode(kvt)
    return { offset: record.offset, value: newRecBuffer }
  }

  function decrypt(record, streaming) {
    const recOffset = record.offset
    const recBuffer = record.value
    if (!recBuffer) return record
    if (decryptedIdx.has(recOffset)) {
      const pValue = bipf.seekKey2(recBuffer, 0, BIPF_VALUE, 0)
      if (pValue < 0) return record
      const pContent = bipf.seekKey2(recBuffer, pValue, BIPF_CONTENT, 0)
      if (pContent < 0) return record

      const ciphertext = bipf.decode(recBuffer, pContent)
      const decryptedRecord = decryptAndReconstruct(ciphertext, record, pValue)
      if (!decryptedRecord) return record

      return decryptedRecord
    } else if (recOffset > latestOffset.value || !streaming) {
      if (streaming) latestOffset.set(recOffset)

      const pValue = bipf.seekKey2(recBuffer, 0, BIPF_VALUE, 0)
      if (pValue < 0) return record
      const pContent = bipf.seekKey2(recBuffer, pValue, BIPF_CONTENT, 0)
      if (pContent < 0) return record

      const type = bipf.getEncodedType(recBuffer, pContent)
      if (type !== bipf.types.string) return record

      const ciphertext = bipf.decode(recBuffer, pContent)

      const encryptionFormat = sbot.db.findEncryptionFormatFor(ciphertext)
      if (!encryptionFormat) return record

      // Special optimization specific to box1
      if (encryptionFormat.name === 'box1' && startDecryptBox1) {
        const pTimestamp = bipf.seekKey2(recBuffer, pValue, BIPF_TIMESTAMP, 0)
        const declaredTimestamp = bipf.decode(recBuffer, pTimestamp)
        if (declaredTimestamp < startDecryptBox1) return record
      }

      if (streaming) {
        const encryptedIdx = encryptedIdxMap.get(encryptionFormat.name)
        encryptedIdx.insert(recOffset)
      }

      const decryptedRecord = decryptAndReconstruct(ciphertext, record, pValue)
      if (!decryptedRecord) return record

      decryptedIdx.insert(recOffset)
      encryptedIdxMap.get(encryptionFormat.name).remove(recOffset)

      if (!streaming) saveIndexes(() => {})
      return decryptedRecord
    } else {
      return record
    }
  }

  function getDecryptedOffsets() {
    return decryptedIdx.all()
  }

  function getEncryptedOffsets(formatName) {
    const idx = encryptedIdxMap.get(formatName)
    if (!idx) return []
    return idx.all()
  }

  function reset(cb) {
    for (const idx of encryptedIdxMap.values()) {
      idx.reset()
    }
    decryptedIdx.reset()
    latestOffset.set(-1)
    saveIndexes(cb)
  }

  return {
    latestOffset,
    decrypt,
    getDecryptedOffsets,
    getEncryptedOffsets,
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
