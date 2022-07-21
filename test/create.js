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
const { where, isDecrypted, toCallback } = require('../operators')

const dir = '/tmp/ssb-db2-create'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

let sbot = SecretStack({ appKey: caps.shs })
  .use(require('../'))
  .use(require('../compat/ebt'))
  .use(require('ssb-bendy-butt'))
  .use(require('ssb-buttwoo'))
  .call(null, {
    keys,
    path: dir,
  })
let db = sbot.db

test('create() classic', (t) => {
  db.create(
    {
      feedFormat: 'classic',
      content: { type: 'post', text: 'I am hungry' },
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

test('create() classic box', (t) => {
  db.create(
    {
      feedFormat: 'classic',
      content: { type: 'post', text: 'I am chewing food' },
      recps: [keys.id],
      encryptionFormat: 'box',
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

  sbot.box2.setOwnDMKey(testkey)

  db.create(
    {
      feedFormat: 'classic',
      content: { type: 'post', text: 'I am drinking milk' },
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
      feedFormat: 'classic',
      content: { type: 'post', text: 'I am drinking beer' },
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
      feedFormat: 'bendybutt-v1',
      content: {
        type: 'metafeed/add/existing',
        feedpurpose: 'main',
        subfeed: mainKeys.id,
        metafeed: mfKeys.id,
      },
      keys: mfKeys,
      contentKeys: mainKeys,
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

test('create() bendybutt-v1 box', (t) => {
  const chessKeys = ssbKeys.generate()
  const mfKeys = ssbKeys.generate(null, null, 'bendybutt-v1')

  db.create(
    {
      feedFormat: 'bendybutt-v1',
      content: {
        type: 'metafeed/add/derived',
        feedpurpose: 'chess',
        subfeed: chessKeys.id,
        metafeed: mfKeys.id,
        nonce: Buffer.alloc(32, 3),
      },
      keys: mfKeys,
      contentKeys: chessKeys,
      recps: [mfKeys.id, keys.id],
      encryptionFormat: 'box',
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

  sbot.box2.addKeypair(mfKeys)

  db.create(
    {
      feedFormat: 'bendybutt-v1',
      content: {
        type: 'metafeed/add/derived',
        feedpurpose: 'chess',
        subfeed: chessKeys.id,
        metafeed: mfKeys.id,
        nonce: Buffer.alloc(32, 3),
      },
      keys: mfKeys,
      contentKeys: chessKeys,
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
      feedFormat: 'buttwoo-v1',
      content: {
        type: 'post',
        text: 'I am the future of scuttlebutt',
      },
      keys: buttwooKeys,
      parent:
        'ssb:message/buttwoo-v1/Xnbc3Ihuslpx8peGO52c1-s59vufH9R5JnxT04vksnA=',
      tag: 0,
    },
    (err, msg1) => {
      t.error(err, 'no err')
      t.equal(msg1.value.content.text, 'I am the future of scuttlebutt')
      t.equal(msg1.value.author, buttwooKeys.id, 'author correct')
      t.ok(msg1.feed, 'kvtf has feed')
      t.notEquals(msg1.value.author, msg1.feed, 'kvtf is not msgVal.author')

      db.create(
        {
          feedFormat: 'buttwoo-v1',
          content: {
            type: 'post',
            text: 'I am the future of scuttlebutt 2',
          },
          keys: buttwooKeys,
          parent:
            'ssb:message/buttwoo-v1/Xnbc3Ihuslpx8peGO52c1-s59vufH9R5JnxT04vksnA=',
          tag: 0,
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

test('create() buttwoo-v1 box', (t) => {
  const buttwooKeys = ssbKeys.generate(null, null, 'buttwoo-v1')

  db.create(
    {
      feedFormat: 'buttwoo-v1',
      content: {
        type: 'post',
        text: 'I am the secret future of ssb',
      },
      keys: buttwooKeys,
      tag: 0,
      recps: [buttwooKeys.id, keys.id],
      encryptionFormat: 'box',
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

  sbot.box2.addKeypair(buttwooKeys)

  db.create(
    {
      feedFormat: 'buttwoo-v1',
      content: {
        type: 'post',
        text: 'Heat death of the universe',
      },
      keys: buttwooKeys,
      tag: 0,
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
        'ssb:feed/buttwoo-v1/LDNR3UuGafiJ63LUNocw5ifmUbtakz7x4_LnBiazXU0=',
      parent: null,
      sequence: 1,
      timestamp: 1654703993976,
      previous: null,
      tag: Buffer.from([0]),
      content: { type: 'post', text: 'Hello world!' },
      contentHash: Buffer.from([
        0, 251, 214, 41, 18, 80, 206, 141, 111, 36, 238, 25, 174, 170, 24, 214,
        117, 232, 5, 114, 61, 21, 126, 176, 187, 182, 171, 62, 43, 192, 11, 225,
        132,
      ]),
      signature: Buffer.from([
        98, 240, 159, 176, 137, 24, 6, 37, 18, 54, 19, 211, 32, 140, 82, 36,
        137, 176, 38, 175, 252, 35, 122, 47, 91, 62, 146, 165, 91, 35, 124, 206,
        177, 50, 187, 239, 4, 157, 177, 170, 137, 248, 141, 101, 80, 106, 30,
        190, 22, 15, 232, 138, 202, 194, 110, 167, 21, 246, 218, 26, 126, 41,
        70, 14,
      ]),
    },
    'js'
  )

  db.add(nativeMsg1, (err, msg) => {
    t.error(err, 'no err')
    t.equal(msg.value.content.text, 'Hello world!')

    const nativeMsg2 = feedFormat.toNativeMsg(
      {
        author:
          'ssb:feed/buttwoo-v1/LDNR3UuGafiJ63LUNocw5ifmUbtakz7x4_LnBiazXU0=',
        parent: null,
        sequence: 2,
        timestamp: 1654703993981,
        previous:
          'ssb:message/buttwoo-v1/FGWB6gHPUVmYQK9C3LOkz1I1_WyPuLStNYvtUPtqv5I=',
        tag: Buffer.from([0]),
        content: { type: 'post', text: 'Hi :)' },
        contentHash: Buffer.from([
          0, 58, 220, 84, 198, 242, 95, 238, 97, 52, 24, 105, 116, 210, 81, 9,
          64, 204, 248, 200, 154, 239, 39, 105, 59, 185, 164, 167, 167, 216,
          122, 252, 113,
        ]),
        signature: Buffer.from([
          160, 47, 188, 243, 200, 37, 218, 186, 43, 98, 225, 187, 10, 50, 164,
          226, 193, 159, 245, 86, 172, 50, 178, 153, 144, 252, 201, 89, 96, 139,
          39, 75, 52, 76, 125, 165, 133, 131, 70, 67, 150, 152, 162, 168, 225,
          132, 21, 246, 18, 59, 158, 174, 252, 111, 120, 62, 212, 104, 84, 212,
          120, 163, 130, 2,
        ]),
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

test('query box', (t) => {
  db.query(
    where(isDecrypted('box')),
    toCallback((err, msgs) => {
      t.error(err, 'no err')
      t.equal(msgs.length, 4)
      t.end()
    })
  )
})

test('query box2', (t) => {
  db.query(
    where(isDecrypted('box2')),
    toCallback((err, msgs) => {
      t.error(err, 'no err')
      t.equal(msgs.length, 3)
      t.end()
    })
  )
})

test('query anybox', (t) => {
  db.query(
    where(isDecrypted()),
    toCallback((err, msgs) => {
      t.error(err, 'no err')
      t.equal(msgs.length, 7)
      t.end()
    })
  )
})

test('teardown', (t) => {
  sbot.close(t.end)
})
