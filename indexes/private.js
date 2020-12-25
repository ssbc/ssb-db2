const Obv = require('obv')
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
  let latestOffset = Obv()
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

  const bValue = Buffer.from('value')
  const bContent = Buffer.from('content')
  const StringType = 0

  function reconstructMessage(record, unboxedContent) {
    let msg = bipf.decode(record.value, 0)
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

  function decrypt(record, streaming) {
    if (bsb.eq(canDecrypt, record.offset) !== -1) {
      let p = 0 // note you pass in p!

      p = bipf.seekKey(record.value, p, bValue)
      if (p >= 0) {
        const pContent = bipf.seekKey(record.value, p, bContent)
        if (pContent >= 0) {
          const content = ssbKeys.unbox(bipf.decode(record.value, pContent), keys)
          if (content) return reconstructMessage(record, content)
        }
      }
    } else if (record.offset > latestOffset.value) {
      if (streaming) latestOffset.set(record.offset)

      let p = 0 // note you pass in p!

      p = bipf.seekKey(record.value, p, bValue)
      if (p >= 0) {
        const pContent = bipf.seekKey(record.value, p, bContent)
        if (pContent >= 0) {
          const type = bipf.getEncodedType(record.value, pContent)
          if (type === StringType) {
            encrypted.push(record.offset)

            const content = ssbKeys.unbox(
              bipf.decode(record.value, pContent),
              keys
            )

            if (content) {
              canDecrypt.push(record.offset)
              return reconstructMessage(record, content)
            }
          }
        }
      }
    }

    return record
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
