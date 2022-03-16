// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const pull = require('pull-stream')
const mkdirp = require('mkdirp')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')

const dir = '/tmp/ssb-db2-about-self-index'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

let sbot = SecretStack({ appKey: caps.shs })
  .use(require('../'))
  .use(require('../about-self'))
  .call(null, {
    keys,
    path: dir,
  })
const db = sbot.db

test('set publicWebHosting to false', (t) => {
  const about = { type: 'about', about: sbot.id, publicWebHosting: false }

  db.publish(about, (err, postMsg) => {
    t.error(err, 'no err')

    sbot.db.onDrain('aboutSelf', () => {
      const profile = sbot.db.getIndex('aboutSelf').getProfile(sbot.id)
      t.false(profile.publicWebHosting)

      t.end()
    })
  })
})

test('set publicWebHosting to true', (t) => {
  const about = { type: 'about', about: sbot.id, publicWebHosting: true }

  db.publish(about, (err, postMsg) => {
    t.error(err, 'no err')

    sbot.db.onDrain('aboutSelf', () => {
      const profile = sbot.db.getIndex('aboutSelf').getProfile(sbot.id)
      t.true(profile.publicWebHosting)

      t.end()
    })
  })
})

test('get self assigned', (t) => {
  const about = { type: 'about', about: sbot.id, name: 'arj', image: '%blob' }
  const aboutOther = { type: 'about', about: '@other', name: 'staltz' }

  db.publish(about, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(aboutOther, (err) => {
      t.error(err, 'no err')

      sbot.db.onDrain('aboutSelf', () => {
        const profile = sbot.db.getIndex('aboutSelf').getProfile(sbot.id)
        t.equal(profile.name, about.name)
        t.equal(profile.image, about.image)

        const newAbout = {
          type: 'about',
          about: sbot.id,
          name: 'arj2',
          image: {
            link: '%blob',
            size: 1024,
          },
        }

        db.publish(newAbout, (err) => {
          t.error(err, 'no err')

          sbot.db.onDrain('aboutSelf', () => {
            const profile = sbot.db.getIndex('aboutSelf').getProfile(sbot.id)
            t.equal(profile.name, newAbout.name)
            t.equal(profile.image, newAbout.image.link)

            t.end()
          })
        })
      })
    })
  })
})

test('get live profile', (t) => {
  const about = { type: 'about', about: sbot.id, name: 'arj', image: '%blob' }
  const aboutOther = { type: 'about', about: '@other', name: 'staltz' }

  db.publish(about, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(aboutOther, (err) => {
      t.error(err, 'no err')

      sbot.db.onDrain('aboutSelf', () => {
        const profile = sbot.db.getIndex('aboutSelf').getProfile(sbot.id)
        t.equal(profile.name, about.name)
        t.equal(profile.image, about.image)

        const newAbout = { type: 'about', about: sbot.id, name: 'arj03' }

        pull(
          sbot.db.getIndex('aboutSelf').getLiveProfile(sbot.id),
          pull.drain((profile) => {
            t.equal(profile.name, newAbout.name)
            t.equal(profile.image, about.image)
            t.end()
          })
        )

        db.publish(newAbout, (err) => {
          t.error(err, 'no err')
        })
      })
    })
  })
})

test('should load about-self from disk', (t) => {
  sbot.close((err) => {
    t.error(err)
    t.pass('closed sbot')
    sbot = SecretStack({ appKey: caps.shs })
      .use(require('../'))
      .use(require('../about-self'))
      .call(null, {
        keys,
        path: dir,
      })

    sbot.db.onDrain('aboutSelf', () => {
      const profiles = sbot.db.getIndex('aboutSelf').getProfiles()
      t.equal(profiles[sbot.id].name, 'arj03')
      t.end()
    })
  })
})

test('teardown sbot', (t) => {
  sbot.close(() => t.end())
})
