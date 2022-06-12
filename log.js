// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const OffsetLog = require('async-append-only-log')
const bipf = require('bipf')
const TooHot = require('too-hot')
const { BLOCK_SIZE, newLogPath, tooHotOpts } = require('./defaults')

const BIPF_AUTHOR = bipf.allocAndEncode('author')
const BIPF_VALUE = bipf.allocAndEncode('value')

module.exports = function (dir, config, privateIndex, db) {
  config = config || {}
  config.db2 = config.db2 || {}

  const log = OffsetLog(newLogPath(dir), {
    blockSize: BLOCK_SIZE,
    validateRecord: (d) => {
      try {
        bipf.decode(d, 0)
        return true
      } catch (ex) {
        return false
      }
    },
  })

  log.add = function (key, value, feedId, encoding, cb) {
    if (encoding !== 'js' && encoding !== 'bipf') {
      // prettier-ignore
      throw new Error('Cannot add msg to the log for unsupported encoding: ' + encoding)
    }

    if (encoding === 'bipf') {
      bipf.markIdempotent(value)
    }

    const kvt = {
      key,
      value,
      timestamp: Date.now(),
    }
    if (feedId !== value.author) kvt.feed = feedId
    const recBuffer = bipf.allocAndEncode(kvt)

    log.append(recBuffer, (err) => {
      if (err) cb(err)
      else cb(null, kvt)
    })
  }

  log.addTransaction = function (keys, values, encoding, cb) {
    if (encoding !== 'js' && encoding !== 'bipf') {
      // prettier-ignore
      throw new Error('Cannot addTransaction to the log for unsupported encoding: ' + encoding)
    }
    if (encoding === 'bipf') {
      for (const value of values) {
        bipf.markIdempotent(value)
      }
    }

    let recBuffers = []
    let kvts = []

    for (let i = 0; i < keys.length; ++i) {
      const kvt = {
        key: keys[i],
        value: values[i],
        timestamp: Date.now(),
      }
      const recBuffer = bipf.allocAndEncode(kvt)
      recBuffers.push(recBuffer)
      kvts.push(kvt)
    }

    log.appendTransaction(recBuffers, (err) => {
      if (err) cb(err)
      else cb(null, kvts)
    })
  }

  // monkey-patch log.get to decrypt the msg
  const originalGet = log.get
  log.get = function (offset, cb) {
    originalGet(offset, (err, buffer) => {
      if (err) return cb(err)
      else {
        const record = { offset, value: buffer }
        cb(null, privateIndex.decrypt(record, false).value)
      }
    })
  }

  // in case you want the encrypted msg
  log.getRaw = originalGet

  log.getNativeMsg = function getNativeMsg(offset, feedFormat, cb) {
    originalGet(offset, (err, buffer) => {
      if (err) return cb(err)

      const pValue = bipf.seekKey2(buffer, 0, BIPF_VALUE, 0)

      let format
      if (!feedFormat) {
        const pValueAuthor = bipf.seekKey2(buffer, pValue, BIPF_AUTHOR, 0)
        const author = bipf.decode(buffer, pValueAuthor)
        format = db.findFeedFormatForAuthor(author)
        if (!format) {
          // prettier-ignore
          return cb(new Error('getNativeMsg() failed because this author is for an unknown feed format: ' + author))
        }
      } else if (typeof feedFormat === 'string') {
        format = db.findFeedFormatByName(feedFormat)
        if (!format) {
          // prettier-ignore
          return cb(new Error('getNativeMsg() failed because this feed format is unknown: ' + feedFormat))
        }
      } else {
        // prettier-ignore
        return cb(new Error('getNativeMsg() failed because the feedFormat is not a string: ' + feedFormat))
      }

      let nativeMsg
      if (format.encodings.includes('bipf')) {
        const valueBuf = bipf.pluck(buffer, pValue)
        nativeMsg = format.toNativeMsg(valueBuf, 'bipf')
      } else {
        const msgVal = bipf.decode(buffer, pValue)
        nativeMsg = format.toNativeMsg(msgVal, 'js')
      }
      cb(null, nativeMsg)
    })
  }

  // monkey-patch log.stream to temporarily pause when the CPU is too busy,
  // and to decrypt the msg
  const originalStream = log.stream
  log.stream = function (opts) {
    const shouldDecrypt = opts.decrypt === false ? false : true
    const tooHot = config.db2.maxCpu ? TooHot(tooHotOpts(config)) : () => false
    const s = originalStream(opts)
    const originalPipe = s.pipe.bind(s)
    s.pipe = function pipe(o) {
      let originalWrite = o.write.bind(o)
      o.write = (record) => {
        const hot = tooHot()
        if (hot && !s.sink.paused) {
          s.sink.paused = true
          hot.then(() => {
            if (shouldDecrypt) originalWrite(privateIndex.decrypt(record, true))
            else originalWrite(record)
            s.sink.paused = false
            s.resume()
          })
        } else {
          if (shouldDecrypt) originalWrite(privateIndex.decrypt(record, true))
          else originalWrite(record)
        }
      }
      return originalPipe(o)
    }
    return s
  }

  return log
}
