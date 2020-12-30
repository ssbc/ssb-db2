const Obv = require('obz')
const bipf = require('bipf')
const fic = require('fastintcompression')
const bsb = require('binary-search-bounds')
const { readFile, writeFile } = require('atomically-universal')
const toBuffer = require('typedarray-to-buffer')
const ssbKeys = require('ssb-keys')
const DeferredPromise = require('p-defer')
const path = require('path')
const Debug = require('debug')

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

    writeFile(filename, b, { fsyncWait: false })
  }

  function load(filename, cb) {
    readFile(filename)
      .then((buf) => {
        if (!buf) return cb(new Error("empty file"))
        const offset = buf.readInt32LE(0)
        const body = buf.slice(4)

        cb(null, { offset, arr: fic.uncompress(body) })
      })
      .catch(cb)
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

  const bValue = Buffer.from('value')
  const bContent = Buffer.from('content')

  function decrypt(record, streaming) {
    const recOffset = record.offset
    const recBuffer = record.value
    let p = 0 // note you pass in p!
    if (bsb.eq(canDecrypt, recOffset) !== -1) {
      p = bipf.seekKey(recBuffer, p, bValue)
      if (p < 0) return record
      p = bipf.seekKey(recBuffer, p, bContent)
      if (p < 0) return record

      const unboxedContent = ssbKeys.unbox(bipf.decode(recBuffer, p), keys)
      if (!unboxedContent) return record

      return reconstructMessage(record, unboxedContent)
    } else if (recOffset > latestOffset.value) {
      if (streaming) latestOffset.set(recOffset)

      p = bipf.seekKey(recBuffer, p, bValue)
      if (p < 0) return record
      p = bipf.seekKey(recBuffer, p, bContent)
      if (p < 0) return record

      const type = bipf.getEncodedType(recBuffer, p)
      if (type !== bipf.types.string) return record

      encrypted.push(recOffset)
      const unboxedContent = ssbKeys.unbox(bipf.decode(recBuffer, p), keys)
      if (!unboxedContent) return record

      canDecrypt.push(recOffset)
      return reconstructMessage(record, unboxedContent)
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
