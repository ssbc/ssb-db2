// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const bipf = require('bipf')
const pull = require('pull-stream')
const pl = require('pull-level')
const clarify = require('clarify-error')
const Plugin = require('./plugin')

const BIPF_AUTHOR = bipf.allocAndEncode('author')
const BIPF_CONTENT = bipf.allocAndEncode('content')
const BIPF_TYPE = bipf.allocAndEncode('type')
const B_ABOUT = Buffer.from('about')

// feedId => hydratedAboutObj
module.exports = class AboutSelf extends Plugin {
  constructor(log, dir) {
    super(log, dir, 'aboutSelf', 4, 'json', 'json')
    this.profiles = {}
  }

  onLoaded(cb) {
    pull(
      pl.read(this.level, {
        gte: '',
        lte: undefined,
        keyEncoding: this.keyEncoding,
        valueEncoding: this.valueEncoding,
        keys: true,
      }),
      pull.drain(
        (data) => (this.profiles[data.key] = data.value),
        (err) => {
          // prettier-ignore
          if (err && err !== true) cb(clarify(err, 'AboutSelf.onLoaded() failed'))
          else cb()
        }
      )
    )
  }

  processRecord(record, seq, pValue) {
    const buf = record.value

    const pAuthor = bipf.seekKey2(buf, pValue, BIPF_AUTHOR, 0)
    const pContent = bipf.seekKey2(buf, pValue, BIPF_CONTENT, 0)
    if (pContent < 0) return
    const pType = bipf.seekKey2(buf, pContent, BIPF_TYPE, 0)
    if (pType < 0) return

    if (bipf.compareString(buf, pType, B_ABOUT) === 0) {
      const author = bipf.decode(buf, pAuthor)
      const content = bipf.decode(buf, pContent)
      if (content.about !== author) return

      this.updateProfileData(author, content)

      this.batch.push({
        type: 'put',
        key: author,
        value: this.profiles[author],
      })
    }
  }

  indexesContent() {
    return true
  }

  reset() {
    this.profiles = {}
  }

  updateProfileData(author, content) {
    let profile = this.profiles[author] || {}

    if (isString(content.name)) profile.name = content.name

    if (isString(content.description)) profile.description = content.description

    if (content.image && isString(content.image.link))
      profile.image = content.image.link
    else if (isString(content.image)) profile.image = content.image

    if (isBoolean(content.publicWebHosting))
      profile.publicWebHosting = content.publicWebHosting

    this.profiles[author] = profile
  }

  getProfile(feedId) {
    return this.profiles[feedId] || {}
  }

  getLiveProfile(feedId) {
    return pl.read(this.level, {
      gte: feedId,
      lte: feedId,
      keyEncoding: this.keyEncoding,
      valueEncoding: this.valueEncoding,
      keys: false,
      live: true,
      old: false,
    })
  }

  getProfiles() {
    return this.profiles
  }
}

function isString(str) {
  return typeof str === 'string'
}

function isBoolean(bool) {
  return typeof str === 'boolean'
}
