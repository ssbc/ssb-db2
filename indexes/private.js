const bipf = require('bipf')
const fic = require('fastintcompression')
const bsb = require('binary-search-bounds')
const { readFile, writeFile } = require('atomically-universal')
const toBuffer = require('typedarray-to-buffer')
const ssbKeys = require('ssb-keys')
const path = require('path')
const Debug = require('debug')

const { unboxKey, unboxBody } = require('envelope-js')
const { keySchemes } = require('private-group-spec')
const { FeedId, MsgId } = require('ssb-tribes/lib/cipherlinks')
const directMessageKey = require('ssb-tribes/lib/direct-message-key')

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
  const bAuthor = Buffer.from('author')
  const bPrevious = Buffer.from('previous')
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

  const buildDMKey = directMessageKey.easy(keys)

  function getDMKey(author, scheme) {
    return { key: buildDMKey(author), scheme }
  }

  function decryptBox2(ciphertext, author, previous) {
    const envelope = Buffer.from(ciphertext.replace('.box2', ''), 'base64')
    const feed_id = new FeedId(author).toTFK()
    const prev_msg_id = new MsgId(previous).toTFK()

    // FIXME: we need a key store here

    // FIXME: group keys
    //const trial_group_keys = keystore.author.groupKeys(author)

    // start with group key because they only check the first slot

    const trial_dm_keys = [
      getDMKey(author, keySchemes.feed_id_dm),
      getDMKey(keys.id, keySchemes.feed_id_self),
    ]

    const read_key = unboxKey(envelope, feed_id, prev_msg_id, trial_dm_keys, {
      maxAttempts: 16,
    })
    if (!read_key) return ''

    const plaintext = unboxBody(envelope, feed_id, prev_msg_id, read_key)
    if (plaintext) return JSON.parse(plaintext.toString('utf8'))
    else return ''
  }

  function decryptBox1(ciphertext, keys) {
    return ssbKeys.unbox(ciphertext, keys)
  }

  function tryDecryptContent(ciphertext, data, pValue) {
    let content = ''
    if (ciphertext.endsWith('.box')) content = decryptBox1(ciphertext, keys)
    else if (ciphertext.endsWith('.box2')) {
      const pAuthor = bipf.seekKey(data.value, pValue, bAuthor)
      if (pAuthor >= 0) {
        const author = bipf.decode(data.value, pAuthor)
        const pPrevious = bipf.seekKey(data.value, pValue, bPrevious)
        if (pPrevious >= 0) {
          const previousMsg = bipf.decode(data.value, pPrevious)
          content = decryptBox2(ciphertext, author, previousMsg)
        }
      }
    }
    return content
  }

  function decrypt(data, streaming) {
    if (bsb.eq(canDecrypt, data.seq) !== -1) {
      let p = 0 // note you pass in p!

      const pValue = bipf.seekKey(data.value, p, bValue)
      if (pValue >= 0) {
        const pContent = bipf.seekKey(data.value, pValue, bContent)
        if (pContent >= 0) {
          const ciphertext = bipf.decode(data.value, pContent)
          const content = tryDecryptContent(ciphertext, data, pValue)

          if (content) {
            const originalMsg = reconstructMessage(data, content)
            return originalMsg
          }
        }
      }
    } else if (data.seq > latestSeq) {
      if (streaming) latestSeq = data.seq

      let p = 0 // note you pass in p!

      let pValue = bipf.seekKey(data.value, p, bValue)
      if (pValue >= 0) {
        const pContent = bipf.seekKey(data.value, pValue, bContent)
        if (pContent >= 0) {
          const type = bipf.getEncodedType(data.value, pContent)
          if (type === StringType) {
            encrypted.push(data.seq)

            const ciphertext = bipf.decode(data.value, pContent)
            const content = tryDecryptContent(ciphertext, data, pValue)

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
