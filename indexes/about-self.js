// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const bipf = require('bipf')
const pull = require('pull-stream')
const pl = require('pull-level')
const Plugin = require('./plugin')

const B_VALUE = Buffer.from('value')
const B_AUTHOR = Buffer.from('author')
const B_CONTENT = Buffer.from('content')
const B_TYPE = Buffer.from('type')
const B_ABOUT = Buffer.from('about')

// feedId => hydratedAboutObj
module.exports = class AboutSelf extends Plugin {
  constructor(log, dir) {
    super(log, dir, 'aboutSelf', 3, 'json', 'json')
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
      pull.drain((data) => (this.profiles[data.key] = data.value), cb)
    )
  }

  processRecord(record, seq) {
    const buf = record.value

    let p = 0 // note you pass in p!
    p = bipf.seekKey(buf, p, B_VALUE)
    if (p < 0) return
    const pAuthor = bipf.seekKey(buf, p, B_AUTHOR)
    const pContent = bipf.seekKey(buf, p, B_CONTENT)
    if (pContent < 0) return
    const pType = bipf.seekKey(buf, pContent, B_TYPE)
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

  updateProfileData(author, content) {
    let profile = this.profiles[author] || {}

    if (content.name) profile.name = content.name

    if (content.description) profile.description = content.description

    if (content.image && typeof content.image.link === 'string')
      profile.image = content.image.link
    else if (typeof content.image === 'string') profile.image = content.image

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
