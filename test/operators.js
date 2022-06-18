// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const fs = require('fs')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const bendyButt = require('ssb-bendy-butt/format')
const pull = require('pull-stream')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const {
  and,
  where,
  type,
  isDecrypted,
  isEncrypted,
  isPublic,
  toCallback,
  author,
  isRoot,
  votesFor,
  mentions,
  hasRoot,
  hasFork,
  hasBranch,
  live,
  toPullStream,
  contact,
  about,
} = require('../operators')

const dir = '/tmp/ssb-db2-operators'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const sbot = SecretStack({ appKey: caps.shs })
  .use(require('../'))
  .use(require('../full-mentions'))
  .use(require('ssb-bendy-butt'))
  .call(null, {
    keys,
    path: dir,
  })
const db = sbot.db

test('dedicated author (opt-in) and dedicated type (default)', (t) => {
  const post = { type: 'dogs', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.query(
      where(and(type('dogs'), author(keys.id, { dedicated: true }))),
      toCallback((err2, msgs) => {
        t.error(err2, 'no err2')
        t.equal(msgs.length, 1)
        t.equal(msgs[0].value.content.type, 'dogs')
        setTimeout(() => {
          const dedicatedAuthorIndex = fs
            .readdirSync(path.join(dir, 'db2', 'jit'))
            .find((f) => f.startsWith('value_author_@') && f.endsWith('.index'))
          t.ok(dedicatedAuthorIndex, 'dedicated author index exists')

          const dedicatedTypeIndex = fs
            .readdirSync(path.join(dir, 'db2', 'jit'))
            .find((f) => f === 'value_content_type_dogs.index')
          t.ok(dedicatedTypeIndex, 'dedicated type index exists')

          const sharedTypeIndex = fs
            .readdirSync(path.join(dir, 'db2', 'jit'))
            .find((f) => f === 'value_content_type.32prefix')
          t.notOk(sharedTypeIndex, 'shared type index does NOT exist')

          const sharedAuthorIndex = fs
            .readdirSync(path.join(dir, 'db2', 'jit'))
            .find((f) => f === 'value_author.32prefix')
          t.notOk(sharedAuthorIndex, 'shared author index does NOT exist')

          t.end()
        }, 1000)
      })
    )
  })
})

test('non-dedicated author (default) and non-dedicated type (opt-in)', (t) => {
  db.query(
    where(and(type('dogs', { dedicated: false }), author(keys.id))),
    toCallback((err2, msgs) => {
      t.error(err2, 'no err2')
      t.equal(msgs.length, 1)
      t.equal(msgs[0].value.content.type, 'dogs')
      setTimeout(() => {
        const sharedTypeIndex = fs
          .readdirSync(path.join(dir, 'db2', 'jit'))
          .find((f) => f === 'value_content_type.32prefix')
        t.ok(sharedTypeIndex, 'shared type index exists')

        const sharedAuthorIndex = fs
          .readdirSync(path.join(dir, 'db2', 'jit'))
          .find((f) => f === 'value_author.32prefix')
        t.ok(sharedAuthorIndex, 'shared author index exists')

        t.end()
      }, 1000)
    })
  )
})

test('can create a reusable query portion', (t) => {
  const about = { type: 'about', text: 'Testing!' }

  db.publish(about, (err, postMsg) => {
    t.error(err, 'no err')

    const myAbouts = db.query(where(and(type('about'), author(keys.id))))

    db.query(
      myAbouts,
      toCallback((err2, msgs) => {
        t.error(err2, 'no err2')
        t.equal(msgs.length, 1)
        t.equal(msgs[0].value.content.type, 'about')

        pull(
          db.query(myAbouts, toPullStream()),
          pull.collect((err3, msgsAgain) => {
            t.error(err3, 'no err3')
            t.equal(msgsAgain.length, 1)
            t.equal(msgsAgain[0].value.content.type, 'about')
            t.end()
          })
        )
      })
    )
  })
})

test('execute and(type("post"), author(me))', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.query(
      where(and(type('post'), author(keys.id))),
      toCallback((err2, msgs) => {
        t.error(err2, 'no err2')
        t.equal(msgs.length, 1)
        t.equal(msgs[0].value.content.type, 'post')
        t.end()
      })
    )
  })
})

test('author() supports bendy butt URIs', (t) => {
  const mfKeys = ssbKeys.generate(null, 'banana', 'bendybutt-v1')
  const mainKeys = keys

  const bbmsg1 = bendyButt.newNativeMsg({
    keys: mfKeys,
    contentKeys: mainKeys,
    content: {
      type: 'metafeed/add/existing',
      feedpurpose: 'main',
      subfeed: mainKeys.id,
      metafeed: mfKeys.id,
      tangles: {
        metafeed: {
          root: null,
          previous: null,
        },
      },
    },
    previous: null,
    timestamp: Date.now(),
    hmacKey: null,
  })
  const msgKey = bendyButt.getMsgId(bbmsg1)

  db.add(bbmsg1, (err, postMsg) => {
    t.error(err, 'no err')

    db.query(
      where(author(mfKeys.id)),
      toCallback((err, msgs) => {
        t.error(err, 'no err')
        t.equals(msgs.length, 1, 'there is 1 message')
        t.equals(msgs[0].key, msgKey)
        t.equals(msgs[0].value.author, mfKeys.id)
        t.end()
      })
    )
  })
})

test('execute and(type("post"), isDecrypted())', (t) => {
  let content = { type: 'post', text: 'super secret', recps: [keys.id] }
  content = ssbKeys.box(
    content,
    content.recps.map((x) => x.substr(1))
  )

  db.publish(content, (err, postMsg) => {
    t.error(err, 'no err')

    db.query(
      where(and(type('post'), isDecrypted())),
      toCallback((err, msgs) => {
        t.error(err, 'no err')
        t.equal(msgs.length, 1)
        t.equal(msgs[0].value.content.text, 'super secret')
        t.end()
      })
    )
  })
})

test('execute isDecrypted(box1))', (t) => {
  db.query(
    where(and(type('post'), isDecrypted('box1'))),
    toCallback((err, msgs) => {
      t.error(err, 'no err')
      t.equal(msgs.length, 1)
      t.equal(msgs[0].value.content.text, 'super secret')
      t.end()
    })
  )
})

test('execute isDecrypted(box2))', (t) => {
  db.query(
    where(and(type('post'), isDecrypted('box2'))),
    toCallback((err, msgs) => {
      t.error(err, 'no err')
      t.equal(msgs.length, 0)
      t.end()
    })
  )
})

test('execute isEncrypted()', (t) => {
  // Some message copied from the protocol guide
  const msgVal = {
    previous: '%+7u6Fa0s1cE6tS9BtKUijDV3QBYQEINH7gLSIkDqRMM=.sha256',
    author: '@FCX/tsDLpubCPKKfIrw4gc+SQkHcaD17s7GI6i/ziWY=.ed25519',
    sequence: 15,
    timestamp: 1516222868742,
    hash: 'sha256',
    content:
      'ilDLCLIPRruIQPuqOq1uKnkMh0VNmxD8q+DXKCbgThoAR4XvotMSbMYnodhEkgUQuEEbxjR/MHTa77DQKY5QiGbFusUU564iDx1g/tP0qNqwir6eB0LGEna+K5QDj4CtNsHwnmDv7C0p/9n8lq/WtXlKptrO/A6riL+8EfhIWck1KCQGIZNxZz84DtpDXdN1z88rvslDNoPPzQoeGIgkt/RrGsoysuMZoJyN8LZb3XuczoSn+FhS0nWKIYnCy+CtmNiqw+9lATZgXM4+FOY8N3+L+j25hQQI191NNIdFVyMwoxkPL81byqLxABJDLpMDSOXnWjvyzCJ68UOUwciS16/QdXE647xJ4NSC7u6uMreFIdtHTkQcP556PlZyBFwArJXbwxTUq84f5rTUt3uoG3fOllxFjRs/PPLkIcD1ihxJoSmoTTbFePclRYAV5FptRTJVHg==.box',
    signature:
      '6EQTBQbBhAxeE3w7kyg/7xWHUR8tXP7jUl7bWnEQVz8RxbCYgbTRUnfX/2v68xfSG5xyLAqDJ1Dh3+d+pmRvAw==.sig.ed25519',
  }

  db.addOOO(msgVal, (err) => {
    t.error(err, 'no err')

    db.query(
      where(isEncrypted()),
      toCallback((err, msgs) => {
        t.error(err, 'no err')
        t.equal(msgs.length, 1)
        t.equal(msgs[0].value.sequence, 15)
        t.end()
      })
    )
  })
})

test('execute isEncrypted(box1)', (t) => {
  db.query(
    where(isEncrypted('box1')),
    toCallback((err, msgs) => {
      t.error(err, 'no err')
      t.equal(msgs.length, 1)
      t.equal(msgs[0].value.sequence, 15)
      t.end()
    })
  )
})

test('execute isEncrypted(box2)', (t) => {
  db.query(
    where(isEncrypted('box2')),
    toCallback((err, msgs) => {
      t.error(err, 'no err')
      t.equal(msgs.length, 0)
      t.end()
    })
  )
})

test('execute and(type("post"), isPublic())', (t) => {
  let content = { type: 'posty', text: 'super secret', recps: [keys.id] }
  content = ssbKeys.box(
    content,
    content.recps.map((x) => x.substr(1))
  )

  db.publish(content, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish({ type: 'posty', text: 'Testing public' }, (err, postMsg) => {
      db.query(
        where(and(type('posty'), isPublic())),
        toCallback((err2, msgs) => {
          t.error(err2, 'no err2')
          t.equal(msgs.length, 1)
          t.equal(msgs[0].value.content.text, 'Testing public')
          t.end()
        })
      )
    })
  })
})

test('execute isRoot()', (t) => {
  db.publish({ type: 'foo', text: 'Testing root!' }, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(
      { type: 'foo', text: 'Testing reply!', root: postMsg.key },
      (err2) => {
        t.error(err2, 'no err2')

        db.query(
          where(and(type('foo'), isRoot())),
          toCallback((err3, msgs) => {
            t.error(err3, 'no err3')
            t.equal(msgs.length, 1)
            t.equal(msgs[0].value.content.text, 'Testing root!')
            t.end()
          })
        )
      }
    )
  })
})

test('execute hasRoot(msgkey)', (t) => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(post2, (err) => {
      t.error(err, 'no err')

      const threadMsg1 = { type: 'post', text: 'reply', root: postMsg.key }

      db.publish(threadMsg1, (err) => {
        t.error(err, 'no err')

        db.query(
          where(hasRoot(postMsg.key)),
          toCallback((err, results) => {
            t.error(err, 'no err')
            t.equal(results.length, 1)
            t.equal(results[0].value.content.text, threadMsg1.text)
            t.end()
          })
        )
      })
    })
  })
})

test('execute hasFork(msgkey)', (t) => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(post2, (err) => {
      t.error(err, 'no err')

      const threadMsg1 = { type: 'post', text: 'reply', fork: postMsg.key }

      db.publish(threadMsg1, (err) => {
        t.error(err, 'no err')

        db.query(
          where(hasFork(postMsg.key)),
          toCallback((err, results) => {
            t.error(err, 'no err')
            t.equal(results.length, 1)
            t.equal(results[0].value.content.text, threadMsg1.text)
            t.end()
          })
        )
      })
    })
  })
})

test('execute hasBranch(msgkey)', (t) => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(post2, (err) => {
      t.error(err, 'no err')

      const threadMsg1 = { type: 'post', text: 'reply', branch: postMsg.key }

      db.publish(threadMsg1, (err) => {
        t.error(err, 'no err')

        db.query(
          where(hasBranch(postMsg.key)),
          toCallback((err, results) => {
            t.error(err, 'no err')
            t.equal(results.length, 1)
            t.equal(results[0].value.content.text, threadMsg1.text)
            t.end()
          })
        )
      })
    })
  })
})

test('hasRoot() outputs encrypted replies too', (t) => {
  const post = { type: 'post', text: 'Testing!' }
  const post2 = { type: 'post', text: 'Testing 2!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(post2, (err) => {
      t.error(err, 'no err')

      let threadMsg1 = {
        type: 'post',
        text: 'encrypted reply',
        root: postMsg.key,
        recps: [keys.id],
      }
      threadMsg1 = ssbKeys.box(
        threadMsg1,
        threadMsg1.recps.map((x) => x.substr(1))
      )

      db.publish(threadMsg1, (err, privMsg) => {
        t.error(err, 'no err')

        db.query(
          where(hasRoot(postMsg.key)),
          toCallback((err, results) => {
            t.error(err, 'no err')
            t.equal(results.length, 1)
            t.equal(results[0].value.content.text, 'encrypted reply')
            t.end()
          })
        )
      })
    })
  })
})

test('execute mentions(feedid) and mentions(msgkey)', (t) => {
  const post = { type: 'post', text: 'Testing!' }
  const feedId = '@abc'
  const mentionFeed = {
    type: 'post',
    text: 'Hello @abc',
    mentions: [{ link: feedId }],
  }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    db.publish(mentionFeed, (err) => {
      t.error(err, 'no err')

      const mentionMsg = {
        type: 'post',
        text: `What is [this](${postMsg.key})`,
        mentions: [{ link: postMsg.key }],
      }

      db.publish(mentionMsg, (err) => {
        t.error(err, 'no err')

        db.query(
          where(mentions(feedId)),
          toCallback((err, results) => {
            t.error(err, 'no err')
            t.equal(results.length, 1)
            t.equal(results[0].value.content.text, mentionFeed.text)

            db.query(
              where(mentions(postMsg.key)),
              toCallback((err2, results2) => {
                t.error(err2, 'no err')
                t.equal(results2.length, 1)
                t.equal(results2[0].value.content.text, mentionMsg.text)
                t.end()
              })
            )
          })
        )
      })
    })
  })
})

test('execute contact(feedid)', (t) => {
  const feedid = ssbKeys.generate().id
  const msg1 = { type: 'contact', contact: feedid, following: true }
  const msg2 = { type: 'post', text: 'Testing!' }

  db.publish(msg1, (err, m1) => {
    t.error(err, 'no err')
    db.publish(msg2, (err, m2) => {
      t.error(err, 'no err')

      db.query(
        where(contact(feedid)),
        toCallback((err, results) => {
          t.error(err, 'no err')
          t.equal(results.length, 1)
          t.equal(results[0].value.content.following, true)
          t.end()
        })
      )
    })
  })
})

test('execute about(feedid)', (t) => {
  const feedid = ssbKeys.generate().id
  const msg1 = { type: 'about', about: feedid, name: 'Alice' }
  const msg2 = { type: 'post', text: 'Testing!' }

  db.publish(msg1, (err, m1) => {
    t.error(err, 'no err')
    db.publish(msg2, (err, m2) => {
      t.error(err, 'no err')

      db.query(
        where(about(feedid)),
        toCallback((err, results) => {
          t.error(err, 'no err')
          t.equal(results.length, 1)
          t.equal(results[0].value.content.name, 'Alice')
          t.end()
        })
      )
    })
  })
})

test('execute votesFor(msgkey)', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    const voteMsg1 = {
      type: 'vote',
      vote: {
        link: postMsg.key,
        value: 1,
        expression: '❤',
      },
    }

    const voteMsg2 = {
      type: 'vote',
      vote: {
        link: postMsg.key,
        value: -1,
        expression: '❤',
      },
    }

    db.publish(voteMsg1, (err, v1) => {
      t.error(err, 'no err')

      db.publish(voteMsg2, (err, v2) => {
        t.error(err, 'no err')

        db.query(
          where(votesFor(postMsg.key)),
          toCallback((err, results) => {
            t.error(err, 'no err')
            t.equal(results.length, 2)
            t.equal(results[0].key, v1.key)
            t.equal(results[1].key, v2.key)
            t.end()
          })
        )
      })
    })
  })
})

test('operators are exposed', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    pull(
      db.query(
        db.operators.where(db.operators.key(postMsg.key)),
        db.operators.toCallback((err, results) => {
          t.equal(results[0].key, postMsg.key)
          t.end()
        })
      )
    )
  })
})

test('extra operators are exposed', (t) => {
  t.equal(typeof db.operators.fullMentions, 'function')
  t.end()
})

test('live votesFor', (t) => {
  const post = { type: 'post', text: 'Testing!' }

  db.publish(post, (err, postMsg) => {
    t.error(err, 'no err')

    const voteMsg1 = {
      type: 'vote',
      vote: {
        link: postMsg.key,
        value: 1,
        expression: '❤',
      },
    }

    const voteMsg2 = {
      type: 'vote',
      vote: {
        link: postMsg.key,
        value: -1,
        expression: '❤',
      },
    }

    db.publish(voteMsg1, (err, v1) => {
      t.error(err, 'no err')

      let i = 0
      pull(
        db.query(
          where(votesFor(postMsg.key)),
          live({ old: true }),
          toPullStream()
        ),
        pull.drain(
          (result) => {
            if (i++ == 0) {
              t.equal(result.key, v1.key)
              db.publish(voteMsg2, (err, v2) => {
                t.error(err, 'no err')
              })
            } else {
              t.equal(result.value.content.vote.value, -1)
              t.end()
            }
          },
          () => {}
        )
      )
    })
  })
})

test('live alone', (t) => {
  pull(
    db.query(live({ old: true }), toPullStream()),
    pull.take(1),
    pull.drain((msg) => {
      t.ok(msg)
      t.ok(msg.value)
      t.ok(msg.value.content)
      t.end()
    })
  )
})

test('teardown sbot', (t) => {
  sbot.close(() => t.end())
})
