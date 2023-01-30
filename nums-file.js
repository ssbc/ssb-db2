// SPDX-FileCopyrightText: 2022 Andre 'Staltz' Medeiros <contact@staltz.com>
//
// SPDX-License-Identifier: LGPL-3.0-only

const fic = require('fastintcompression')
const bsb = require('binary-search-bounds')
const AtomicFileRW = require('atomic-file-rw')
const toBuffer = require('typedarray-to-buffer')

/**
 * An array of integers that is persisted to disk, and is useful for
 * implementing indexes in a similar way to bitvectors.
 */
class NumsFile {
  constructor(path) {
    this._path = path
    this._arr = []
    this.offset = -1
  }

  saveFile(offset, cb) {
    const buf = toBuffer(fic.compress(this._arr))
    const b = Buffer.alloc(4 + buf.length)
    b.writeInt32LE(offset, 0)
    buf.copy(b, 4)
    this.offset = offset

    AtomicFileRW.writeFile(this._path, b, (err) => {
      // prettier-ignore
      if (err) console.error(new Error('NumsFile failed to save at ' + this._path, {cause: err}))
      if (cb) cb()
    })
  }

  loadFile(cb) {
    AtomicFileRW.readFile(this._path, (err, buf) => {
      if (err) return cb && cb(err)
      else if (!buf) return cb && cb(new Error('Empty NumsFile'))

      const offset = buf.readInt32LE(0)
      const body = buf.slice(4)

      this._arr = fic.uncompress(body)
      this.offset = offset
      if (cb) cb()
    })
  }

  size() {
    return this._arr.length
  }

  has(num) {
    return bsb.eq(this._arr, num) !== -1
  }

  insert(num) {
    if (num > this._arr[this._arr.length - 1]) {
      this._arr.push(num)
    } else {
      const insertLocation = bsb.gt(this._arr, num)
      this._arr.splice(insertLocation, 0, num)
    }
  }

  remove(num) {
    const idx = bsb.eq(this._arr, num)
    if (idx !== -1) {
      this._arr.splice(idx, 1)
    }
  }

  reset() {
    this._arr.length = 0
    this.offset = -1
  }

  all() {
    return this._arr
  }
}

module.exports = NumsFile
