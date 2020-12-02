const bipf = require('bipf')
const fic = require('fastintcompression')
const bsb = require('binary-search-bounds')
const { readFile, writeFile } = require('atomically-universal')
const toBuffer = require('typedarray-to-buffer')
const ssbKeys = require('ssb-keys')
const path = require('path')
const Debug = require('debug')

const { indexesPath } = require('../defaults')

module.exports = function (dir, keys) {
  let latestSeq = -1
  let encrypted = []
  let canDecrypt = []

  const debug = Debug('ssb:db2:private')

  const encryptedFile = path.join(indexesPath(dir), 'encrypted.index')
  const canDecryptFile = path.join(indexesPath(dir), 'canDecrypt.index')

  function save(filename, seq, arr, cb) {
    const buf = toBuffer(fic.compress(arr))
    const b = Buffer.alloc(4 + buf.length)
    b.writeInt32LE(seq, 0)
    buf.copy(b, 4)

    writeFile(filename, b, { fsyncWait: false })
      .then(() => cb())
      .catch((err) => cb(err))
  }

  function load(filename, cb) {
    readFile(filename)
      .then((buf) => {
        const seq = buf.readInt32LE(0)
        const body = buf.slice(4)

        cb(null, { seq, arr: fic.uncompress(body) })
      })
      .catch((err) => cb(err))
  }

  function loadIndexes(cb) {
    load(encryptedFile, (err, data) => {
      if (err) return cb(err)

      const { seq, arr } = data
      latestSeq = seq
      encrypted = arr

      debug('encrypted loaded', encrypted.length)
      debug('latest seq', latestSeq)

      load(canDecryptFile, (err, data) => {
        canDecrypt = data.arr

        debug('canDecrypt loaded', canDecrypt.length)

        cb()
      })
    })
  }

  loadIndexes(() => {})

  function saveIndexes(cb) {
    save(encryptedFile, latestSeq, encrypted, () => {
      save(canDecryptFile, latestSeq, canDecrypt, cb)
    })
  }

  const bKey = Buffer.from('key')
  const bValue = Buffer.from('value')
  const bContent = Buffer.from('content')
  const StringType = 0

  function reconstructMessage(data, unboxedContent) {
    let msg = bipf.decode(data.value, 0)
    const originalContent = msg.value.content
    msg.value.content = unboxedContent
    msg.meta = {
      private: true,
      originalContent,
    }

    const len = bipf.encodingLength(msg)
    const buf = Buffer.alloc(len)
    bipf.encode(msg, buf, 0)

    return { seq: data.seq, value: buf }
  }

  function decrypt(data, streaming) {
    if (bsb.eq(canDecrypt, data.seq) !== -1) {
      let p = 0 // note you pass in p!

      p = bipf.seekKey(data.value, p, bValue)
      if (~p) {
        const pContent = bipf.seekKey(data.value, p, bContent)
        if (~pContent) {
          const content = ssbKeys.unbox(bipf.decode(data.value, pContent), keys)
          if (content) return reconstructMessage(data, content)
        }
      }
    } else if (data.seq > latestSeq) {
      if (streaming) latestSeq = data.seq

      let p = 0 // note you pass in p!

      p = bipf.seekKey(data.value, p, bValue)
      if (~p) {
        const pContent = bipf.seekKey(data.value, p, bContent)
        if (~pContent) {
          const type = bipf.getEncodedType(data.value, pContent)
          if (type === StringType) {
            encrypted.push(data.seq)

            const content = ssbKeys.unbox(
              bipf.decode(data.value, pContent),
              keys
            )

            if (content) {
              canDecrypt.push(data.seq)
              return reconstructMessage(data, content)
            }
          }
        }
      }
    }

    return data
  }

  return {
    decrypt,
    saveIndexes,
  }
}

module.exports.reEncrypt = function (msg) {
  if (msg.meta && msg.meta.private) {
    msg.value.content = msg.meta.originalContent
    delete msg.meta
  }
  return msg
}
