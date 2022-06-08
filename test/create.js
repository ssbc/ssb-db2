// SPDX-FileCopyrightText: 2022 Andre 'Staltz' Medeiros <contact@staltz.com>
//
// SPDX-License-Identifier: Unlicense

const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const bipf = require('bipf')
const mkdirp = require('mkdirp')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')

const dir = '/tmp/ssb-db2-create'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

let sbot = SecretStack({ appKey: caps.shs })
  .use(require('../'))
  .use(require('../compat/ebt'))
  .call(null, {
    keys,
    path: dir,
  })
let db = sbot.db

test('create() classic', (t) => {
  db.create(
    {
      content: { type: 'post', text: 'I am hungry' },
      feedFormat: 'classic',
    },
    (err, msg1) => {
      t.error(err, 'no err')
      t.equal(msg1.value.content.text, 'I am hungry', 'text correct')

      db.create(
        {
          content: { type: 'post', text: 'I am hungry 2' },
          feedFormat: 'classic',
        },
        (err, msg2) => {
          t.error(err, 'no err')
          t.equal(msg2.value.content.text, 'I am hungry 2', 'text correct')
          t.equal(msg2.value.previous, msg1.key, 'previous correct')

          t.end()
        }
      )
    }
  )
})

test('create() classic box1', (t) => {
  db.create(
    {
      content: { type: 'post', text: 'I am chewing food' },
      feedFormat: 'classic',
      recps: [keys.id],
      encryptionFormat: 'box1',
    },
    (err, msgBoxed) => {
      t.error(err, 'no err')
      t.equal(typeof msgBoxed.value.content, 'string')
      t.true(msgBoxed.value.content.endsWith('.box'), '.box')

      db.getMsg(msgBoxed.key, (err, msg) => {
        t.error(err, 'no err')
        t.equals(msg.value.content.text, 'I am chewing food')
        t.end()
      })
    }
  )
})

test('create() classic box2', (t) => {
  const testkey = Buffer.from(
    '30720d8f9cbf37f6d7062826f6decac93e308060a8aaaa77e6a4747f40ee1a76',
    'hex'
  )

  sbot.box2.addOwnDMKey(testkey)

  db.create(
    {
      content: { type: 'post', text: 'I am drinking milk' },
      feedFormat: 'classic',
      recps: [keys.id],
      encryptionFormat: 'box2',
    },
    (err, msgBoxed) => {
      t.error(err, 'no err')
      t.equal(typeof msgBoxed.value.content, 'string')
      t.true(msgBoxed.value.content.endsWith('.box2'), '.box2')

      db.getMsg(msgBoxed.key, (err, msg) => {
        t.error(err, 'no err')
        t.equals(msg.value.content.text, 'I am drinking milk')
        t.end()
      })
    }
  )
})

test('create() classic "any box"', (t) => {
  db.create(
    {
      content: { type: 'post', text: 'I am drinking beer' },
      feedFormat: 'classic',
      recps: [keys.id],
    },
    (err, msgBoxed) => {
      t.error(err, 'no err')
      t.equal(typeof msgBoxed.value.content, 'string')
      t.true(msgBoxed.value.content.endsWith('.box'), '.box')

      db.getMsg(msgBoxed.key, (err, msg) => {
        t.error(err, 'no err')
        t.equals(msg.value.content.text, 'I am drinking beer')
        t.end()
      })
    }
  )
})

test('create() bendybutt-v1', (t) => {
  const mainKeys = ssbKeys.generate()
  const mfKeys = ssbKeys.generate(null, null, 'bendybutt-v1')

  db.create(
    {
      content: {
        type: 'metafeed/add/existing',
        feedpurpose: 'main',
        subfeed: mainKeys.id,
        metafeed: mfKeys.id,
      },
      keys: mfKeys,
      feedFormat: 'bendybutt-v1',
    },
    (err, msg1) => {
      t.error(err, 'no err')
      t.equal(msg1.value.author, mfKeys.id, 'author correct')
      t.equal(msg1.value.content.subfeed, mainKeys.id, 'content correct')

      db.create(
        {
          content: {
            type: 'metafeed/add/derived',
            feedpurpose: 'main',
            subfeed: mainKeys.id,
            metafeed: mfKeys.id,
          },
          keys: mfKeys,
          feedFormat: 'bendybutt-v1',
        },
        (err, msg2) => {
          t.error(err, 'no err')
          t.equal(msg2.value.previous, msg1.key, 'previous correct')

          t.end()
        }
      )
    }
  )
})

test('create() bendybutt-v1 box1', (t) => {
  const chessKeys = ssbKeys.generate()
  const mfKeys = ssbKeys.generate(null, null, 'bendybutt-v1')

  db.create(
    {
      content: {
        type: 'metafeed/add/derived',
        feedpurpose: 'chess',
        subfeed: chessKeys.id,
        metafeed: mfKeys.id,
        nonce: Buffer.alloc(32, 3),
      },
      feedFormat: 'bendybutt-v1',
      keys: mfKeys,
      recps: [mfKeys.id, keys.id],
      encryptionFormat: 'box1',
    },
    (err, msgBoxed) => {
      t.error(err, 'no err')
      t.equal(msgBoxed.value.author, mfKeys.id, 'author correct')
      t.equal(typeof msgBoxed.value.content, 'string')
      t.true(msgBoxed.value.content.endsWith('.box'), '.box')
      db.getMsg(msgBoxed.key, (err, msg) => {
        t.error(err, 'no err')
        t.equals(msg.value.content.feedpurpose, 'chess')
        t.end()
      })
    }
  )
})

test('create() bendybutt-v1 box2', (t) => {
  const chessKeys = ssbKeys.generate()
  const mfKeys = ssbKeys.generate(null, null, 'bendybutt-v1')

  db.create(
    {
      content: {
        type: 'metafeed/add/derived',
        feedpurpose: 'chess',
        subfeed: chessKeys.id,
        metafeed: mfKeys.id,
        nonce: Buffer.alloc(32, 3),
      },
      feedFormat: 'bendybutt-v1',
      keys: mfKeys,
      recps: [mfKeys.id, keys.id],
      encryptionFormat: 'box2',
    },
    (err, msgBoxed) => {
      t.error(err, 'no err')
      t.equal(msgBoxed.value.author, mfKeys.id, 'author correct')
      t.equal(typeof msgBoxed.value.content, 'string')
      t.true(msgBoxed.value.content.endsWith('.box2'), '.box2')
      db.getMsg(msgBoxed.key, (err, msg) => {
        t.error(err, 'no err')
        t.equals(msg.value.content.feedpurpose, 'chess')
        t.end()
      })
    }
  )
})

test('create() buttwoo-v1', (t) => {
  const buttwooKeys = ssbKeys.generate(null, null, 'buttwoo-v1')

  db.create(
    {
      content: {
        type: 'post',
        text: 'I am the future of scuttlebutt',
      },
      parent:
        'ssb:message/buttwoo-v1/Xnbc3Ihuslpx8peGO52c1-s59vufH9R5JnxT04vksnA=',
      keys: buttwooKeys,
      feedFormat: 'buttwoo-v1',
    },
    (err, msg1) => {
      t.error(err, 'no err')
      t.equal(msg1.value.content.text, 'I am the future of scuttlebutt')
      t.equal(msg1.value.author, buttwooKeys.id, 'author correct')
      t.ok(msg1.feed, 'kvtf has feed')
      t.notEquals(msg1.value.author, msg1.feed, 'kvtf is not msgVal.author')

      db.create(
        {
          content: {
            type: 'post',
            text: 'I am the future of scuttlebutt 2',
          },
          parent:
            'ssb:message/buttwoo-v1/Xnbc3Ihuslpx8peGO52c1-s59vufH9R5JnxT04vksnA=',
          keys: buttwooKeys,
          feedFormat: 'buttwoo-v1',
          encoding: 'bipf',
        },
        (err, msg2) => {
          t.error(err, 'no err')
          msg2.value = bipf.decode(msg2.value)
          t.equal(msg2.value.previous, msg1.key, 'previous correct')
          t.end()
        }
      )
    }
  )
})

test('create() buttwoo-v1 box1', (t) => {
  const buttwooKeys = ssbKeys.generate(null, null, 'buttwoo-v1')

  db.create(
    {
      content: {
        type: 'post',
        text: 'I am the secret future of ssb',
      },
      keys: buttwooKeys,
      feedFormat: 'buttwoo-v1',
      recps: [buttwooKeys.id, keys.id],
      encryptionFormat: 'box1',
    },
    (err, msgBoxed) => {
      t.error(err, 'no err')
      t.equal(msgBoxed.value.author, buttwooKeys.id, 'author correct')
      t.true(msgBoxed.value.content.endsWith('.box'), '.box')
      db.getMsg(msgBoxed.key, (err, msg) => {
        t.error(err, 'no err')
        t.equals(msg.value.content.text, 'I am the secret future of ssb')
        t.end()
      })
    }
  )
})

test('create() buttwoo-v1 box2', (t) => {
  const buttwooKeys = ssbKeys.generate(null, null, 'buttwoo-v1')

  db.create(
    {
      content: {
        type: 'post',
        text: 'Heat death of the universe',
      },
      keys: buttwooKeys,
      feedFormat: 'buttwoo-v1',
      recps: [buttwooKeys.id, keys.id],
      encryptionFormat: 'box2',
    },
    (err, msgBoxed) => {
      t.error(err, 'no err')
      t.equal(msgBoxed.value.author, buttwooKeys.id, 'author correct')
      t.true(msgBoxed.value.content.endsWith('.box2'), '.box2')
      db.getMsg(msgBoxed.key, (err, msg) => {
        t.error(err, 'no err')
        t.equals(msg.value.content.text, 'Heat death of the universe')
        t.end()
      })
    }
  )
})

test('add() classic', (t) => {
  const feedFormat = db.findFeedFormatByName('classic')

  const nativeMsg = feedFormat.toNativeMsg(
    {
      previous: null,
      author: '@FCX/tsDLpubCPKKfIrw4gc+SQkHcaD17s7GI6i/ziWY=.ed25519',
      sequence: 1,
      timestamp: 1514517067954,
      hash: 'sha256',
      content: {
        type: 'post',
        text: 'This is the first post!',
      },
      signature:
        'QYOR/zU9dxE1aKBaxc3C0DJ4gRyZtlMfPLt+CGJcY73sv5abKKKxr1SqhOvnm8TY784VHE8kZHCD8RdzFl1tBA==.sig.ed25519',
    },
    'js'
  )

  db.add(nativeMsg, (err, msg) => {
    t.error(err, 'no err')
    t.equal(msg.value.content.text, 'This is the first post!')
    t.end()
  })
})

test('add() bendybutt-v1', (t) => {
  const feedFormat = db.findFeedFormatByName('bendybutt-v1')

  const nativeMsg = feedFormat.toNativeMsg(
    {
      author:
        'ssb:feed/bendybutt-v1/b99R2e7lj8h7NFqGhOu6lCGy8gLxWV-J4ORd1X7rP3c=',
      sequence: 1,
      previous: null,
      timestamp: 0,
      content: {
        type: 'metafeed/add/derived',
        feedpurpose: 'main default',
        subfeed: '@Oo6OYCGsjLP3n+cep4FiHJJZGHyqKWztnhDk7vJhi3A=.ed25519',
        metafeed:
          'ssb:feed/bendybutt-v1/b99R2e7lj8h7NFqGhOu6lCGy8gLxWV-J4ORd1X7rP3c=',
        nonce: Buffer.from([
          35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35,
          35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35,
        ]),
        tangles: { metafeed: { root: null, previous: null } },
      },
      contentSignature:
        'rne0MtsZFEch45RAqaKaQtub81NBoB/o0vHTyrezOmfA2awSzRbeWyVeKA7sODwNfqkH3Vg8uuHrt431G9MWCg==.sig.ed25519',
      signature:
        '59zyqOloMVNU/FmL+mLe56aFgCNsmHftlp5ip95ONTIB/G3NkC43PIoFe0yr409sVO4H73yppdOrysxSVnCfBw==.sig.ed25519',
    },
    'js'
  )

  db.add(nativeMsg, (err, msg) => {
    t.error(err, 'no err')
    t.equal(msg.value.content.feedpurpose, 'main default')
    t.end()
  })
})

test('add buttwoo-v1', (t) => {
  const feedFormat = db.findFeedFormatByName('buttwoo-v1')

  const nativeMsg1 = feedFormat.toNativeMsg(
    {
      author:
        'ssb:feed/buttwoo-v1/2u2sOdtRzFSnAMwXMSIGqMlJpkeU1Wzocpwol1oRwyo=',
      parent: null,
      sequence: 1,
      timestamp: 1654681336060,
      previous: null,
      tag: Buffer.from([0]),
      content: { type: 'post', text: 'Hello world!' },
      contentHash: Buffer.from([
        251, 214, 41, 18, 80, 206, 141, 111, 36, 238, 25, 174, 170, 24, 214,
        117, 232, 5, 114, 61, 21, 126, 176, 187, 182, 171, 62, 43, 192, 11, 225,
        132,
      ]),
      signature:
        'FYFU7uYLsXyXsdZxUfuaXSnrkIQ/HNllDrnyjlDOzWR0SaEhfTkAdFqm77GRpvzeLajku+6RC0yUDohn2esLDA==.sig.ed25519',
    },
    'js'
  )

  db.add(nativeMsg1, (err, msg) => {
    t.error(err, 'no err')
    t.equal(msg.value.content.text, 'Hello world!')

    const nativeMsg2 = feedFormat.toNativeMsg(
      {
        author:
          'ssb:feed/buttwoo-v1/2u2sOdtRzFSnAMwXMSIGqMlJpkeU1Wzocpwol1oRwyo=',
        parent: null,
        sequence: 2,
        timestamp: 1654681336068,
        previous:
          'ssb:message/buttwoo-v1/wzZBU4q8nNtcjpoaZktBeP_z7s_Z9DPQNah_aFy7JQs=',
        tag: Buffer.from([0]),
        content: { type: 'post', text: 'Hi :)' },
        contentHash: Buffer.from([
          58, 220, 84, 198, 242, 95, 238, 97, 52, 24, 105, 116, 210, 81, 9, 64,
          204, 248, 200, 154, 239, 39, 105, 59, 185, 164, 167, 167, 216, 122,
          252, 113,
        ]),
        signature:
          '51HaN7Ng8UeuVpLguRKRpXpF+OcOBdK9MNRtmL77EkJf5OoNukQSSXbUeWqP8jTbBjDM5DaXLLNl5592K4WDDw==.sig.ed25519',
      },
      'js'
    )

    db.add(nativeMsg2, (err, msg) => {
      t.error(err, 'no err')
      t.equal(msg.value.content.text, 'Hi :)')

      t.end()
    })
  })
})

test('teardown', (t) => {
  sbot.close(t.end)
})
