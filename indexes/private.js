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

const { unboxKey, unboxBody } = require('envelope-js')
const { keySchemes } = require('private-group-spec')
const KeyStore = require('ssb-tribes/key-store')
const { FeedId, MsgId } = require('ssb-tribes/lib/cipherlinks')
const directMessageKey = require('ssb-tribes/lib/direct-message-key')
const { isFeed, isCloakedMsg } = require('ssb-ref')

const { indexesPath } = require('../defaults')

module.exports = function (dir, keys, reindex) {
  const latestOffset = Obv()
  const stateLoaded = DeferredPromise()
  let encrypted = []
  let canDecrypt = []

  let tasks = []
  const tasksCompleted = DeferredPromise()

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
        if (!buf) return cb(new Error('empty file'))
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
        debug('loaded offset', latestOffset.value)

        cb()
      })
    })
  }

  let keystore
  loadIndexes((err) => {
    if (err) throw err

    keystore = KeyStore(path.join(dir, 'tribes/keystore'), keys, (err) => {
      if (err) throw err

      //console.log('loaded keystore')
      stateLoaded.resolve()
    })    
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
  const bAuthor = Buffer.from('author')
  const bPrevious = Buffer.from('previous')
  const bContent = Buffer.from('content')

  function decryptBox2Msg(envelope, feed_id, prev_msg_id, read_key) {
    const plaintext = unboxBody(envelope, feed_id, prev_msg_id, read_key)
    if (plaintext) return JSON.parse(plaintext.toString('utf8'))
    else return ''
  }

  function maybeAddGroupMember(msg, author, cb) {
    if (msg && msg.type === 'group/add-member')
    {
      const authors = [
        author,
        ...msg.recps.filter(isFeed)
      ]

      keystore.processAddMember({
        groupId: msg.recps.filter(isCloakedMsg)[0],
        groupKey: msg.groupKey,
        root:  msg.tangles.group.root,
        authors
      }, cb)
    }
  }

  function decryptBox2(ciphertext, author, previous) {
    const envelope = Buffer.from(ciphertext.replace('.box2', ''), 'base64')
    const feed_id = new FeedId(author).toTFK()
    const prev_msg_id = new MsgId(previous).toTFK()

    const trial_group_keys = keystore.author.groupKeys(author)

    let read_key = unboxKey(envelope, feed_id, prev_msg_id, trial_group_keys, {
      maxAttempts: 1,
    })

    if (read_key) {
      const msg = decryptBox2Msg(envelope, feed_id, prev_msg_id, read_key)
      maybeAddGroupMember(msg, author, (err, result) => {
        if (err) console.error(err)
      })
      //console.log("decrypted a msg using group key!", msg)
      return msg
    }

    const trial_dm_keys = [
      keystore.author.sharedDMKey(author),
      ...keystore.ownKeys(),
    ]

    read_key = unboxKey(envelope, feed_id, prev_msg_id, trial_dm_keys, {
      maxAttempts: 16,
    })

    if (read_key) {
      const msg = decryptBox2Msg(envelope, feed_id, prev_msg_id, read_key)
      maybeAddGroupMember(msg, author, (err, newAuthors) => {
        if (err) console.error(err)
        if (author !== keys.id && newAuthors.indexOf(keys.id) != -1)
        {
          const toDecrypt = encrypted.filter(x => !canDecrypt.includes(x))
          reindex(toDecrypt, (err) => {
            tasksCompleted.resolve()
          })
        }
      })
      return msg
    } else return ''
  }

  function decryptBox1(ciphertext, keys) {
    return ssbKeys.unbox(ciphertext, keys)
  }

  function tryDecryptContent(ciphertext, recBuffer, pValue) {
    let content = ''
    if (ciphertext.endsWith('.box')) content = decryptBox1(ciphertext, keys)
    else if (ciphertext.endsWith('.box2')) {
      const pAuthor = bipf.seekKey(recBuffer, pValue, bAuthor)
      if (pAuthor >= 0) {
        const author = bipf.decode(recBuffer, pAuthor)
        const pPrevious = bipf.seekKey(recBuffer, pValue, bPrevious)
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
      const pValue = bipf.seekKey(recBuffer, p, bValue)
      if (pValue < 0) return record
      const pContent = bipf.seekKey(recBuffer, pValue, bContent)
      if (pContent < 0) return record

      const ciphertext = bipf.decode(recBuffer, pContent)
      const content = tryDecryptContent(ciphertext, recBuffer, pValue)
      if (!content) return record

      const originalMsg = reconstructMessage(record, content)
      return originalMsg
    } else if (recOffset > latestOffset.value || !streaming) {
      if (streaming) latestOffset.set(recOffset)

      const pValue = bipf.seekKey(recBuffer, p, bValue)
      if (pValue < 0) return record
      const pContent = bipf.seekKey(recBuffer, pValue, bContent)
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
    onDrain: (cb) => tasksCompleted.promise.then(cb), // FIXME: more complicated
    close: (cb) => keystore ? keystore.close(cb) : cb(),
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
