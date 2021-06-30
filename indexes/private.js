const Obv = require('obz')
const bipf = require('bipf')
const fic = require('fastintcompression')
const bsb = require('binary-search-bounds')
const { readFile, writeFile } = require('atomic-file-rw')
const toBuffer = require('typedarray-to-buffer')
const ssbKeys = require('ssb-keys')
const DeferredPromise = require('p-defer')
const path = require('path')
const Debug = require('debug')

const { unboxKey, unboxBody } = require('envelope-js')
const { ownKey, sharedDMKey } = require('../keystore')

const { indexesPath } = require('../defaults')

module.exports = function (dir, keys) {
  const latestOffset = Obv()
  const stateLoaded = DeferredPromise()
  let encrypted = []
  let canDecrypt = []

  const debug = Debug('ssb:db2:private')

  const encryptedFile = path.join(indexesPath(dir), 'encrypted.index')
  const canDecryptFile = path.join(indexesPath(dir), 'canDecrypt.index')

  function save(filename, arr) {
    const buf = toBuffer(fic.compress(arr))
    const b = Buffer.alloc(4 + buf.length)
    b.writeInt32LE(latestOffset.value, 0)
    buf.copy(b, 4)

    writeFile(filename, b, (err) => {
      if (err) debug("failed to save file %o, got error %o", filename, err)
    })
  }

  function load(filename, cb) {
    readFile(filename, (err, buf) => {
      if (err) return cb(err)
      else if (!buf) return cb(new Error('empty file'))

      const offset = buf.readInt32LE(0)
      const body = buf.slice(4)

      cb(null, { offset, arr: fic.uncompress(body) })
    })
  }

  function loadIndexes(cb) {
    load(encryptedFile, (err, data) => {
      if (err) {
        latestOffset.set(-1)
        stateLoaded.resolve()
        if (err.code === 'ENOENT') cb()
        else if (err.message === 'empty file') cb()
        else cb(err)
        return
      }

      const { offset, arr } = data
      encrypted = arr

      debug('encrypted loaded', encrypted.length)

      load(canDecryptFile, (err, data) => {
        let canDecryptOffset = -1
        if (!err) {
          canDecrypt = data.arr
          canDecryptOffset = data.offset
          debug('canDecrypt loaded', canDecrypt.length)
        }

        latestOffset.set(Math.min(offset, canDecryptOffset))
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
        save(encryptedFile, encrypted)
        save(canDecryptFile, canDecrypt)
      }, 1000)
    }
    cb()
  }

  function reconstructMessage(record, unboxedContent) {
    const msg = bipf.decode(record.value, 0)
    const originalContent = msg.value.content
    msg.value.content = unboxedContent
    msg.meta = {
      private: true,
      originalContent,
    }

    const len = bipf.encodingLength(msg)
    const buf = Buffer.alloc(len)
    bipf.encode(msg, buf, 0)

    return { offset: record.offset, value: buf }
  }

  const B_VALUE = Buffer.from('value')
  const B_CONTENT = Buffer.from('content')
  const B_AUTHOR = Buffer.from('author')
  const B_PREVIOUS = Buffer.from('previous')

  function decryptBox2Msg(envelope, feed_id, prev_msg_id, read_key) {
    const plaintext = unboxBody(envelope, feed_id, prev_msg_id, read_key)
    // FIXME: this assumes that what is boxed is json
    if (plaintext) return JSON.parse(plaintext.toString('utf8'))
    else return ''
  }

  function decryptBox2(ciphertext, author, previous) {
    const envelope = Buffer.from(ciphertext.replace('.box2', ''), 'base64')
    const feed_id = new FeedId(author).toTFK()
    const prev_msg_id = new MsgId(previous).toTFK()

    const trial_dm_keys = [
      sharedDMKey(author),
      ownKey,
    ]

    read_key = unboxKey(envelope, feed_id, prev_msg_id, trial_dm_keys, {
      maxAttempts: 16,
    })

    if (read_key)
      return decryptBox2Msg(envelope, feed_id, prev_msg_id, read_key)
    else return ''
  }

  function decryptBox1(ciphertext, keys) {
    return ssbKeys.unbox(ciphertext, keys)
  }

  function tryDecryptContent(ciphertext, recBuffer, pValue) {
    let content = ''
    if (ciphertext.endsWith('.box')) content = decryptBox1(ciphertext, keys)
    else if (ciphertext.endsWith('.box2')) {
      const pAuthor = bipf.seekKey(recBuffer, pValue, B_AUTHOR)
      if (pAuthor >= 0) {
        const author = bipf.decode(recBuffer, pAuthor)
        const pPrevious = bipf.seekKey(recBuffer, pValue, B_PREVIOUS)
        if (pPrevious >= 0) {
          const previousMsg = bipf.decode(recBuffer, pPrevious)
          content = decryptBox2(ciphertext, author, previousMsg)
        }
      }
    }
    return content
  }

  function decrypt(record, streaming) {
    const recOffset = record.offset
    const recBuffer = record.value
    let p = 0 // note you pass in p!
    if (bsb.eq(canDecrypt, recOffset) !== -1) {
      const pValue = bipf.seekKey(recBuffer, p, B_VALUE)
      if (pValue < 0) return record
      const pContent = bipf.seekKey(recBuffer, pValue, B_CONTENT)
      if (pContent < 0) return record

      const ciphertext = bipf.decode(recBuffer, pContent)
      const content = tryDecryptContent(ciphertext, recBuffer, pValue)
      if (!content) return record

      const originalMsg = reconstructMessage(record, content)
      return originalMsg
    } else if (recOffset > latestOffset.value || !streaming) {
      if (streaming) latestOffset.set(recOffset)

      const pValue = bipf.seekKey(recBuffer, p, B_VALUE)
      if (pValue < 0) return record
      const pContent = bipf.seekKey(recBuffer, pValue, B_CONTENT)
      if (pContent < 0) return record

      const type = bipf.getEncodedType(recBuffer, pContent)
      if (type !== bipf.types.string) return record

      if (streaming)
        encrypted.push(recOffset)

      const ciphertext = bipf.decode(recBuffer, pContent)
      const content = tryDecryptContent(ciphertext, recBuffer, pValue)
      if (!content) return record

      canDecrypt.push(recOffset)
      if (!streaming) saveIndexes(() => {})
      return reconstructMessage(record, content)
    } else {
      return record
    }
  }

  return {
    latestOffset,
    decrypt,
    saveIndexes,
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
