# SSB-DB2

SSB-DB2 is a new database for secure-scuttlebutt, it is meant as a
replacement for [ssb-db]. The main reason for creating a new database
is to be able to rework some of the existing decisions without having
to be 100% backwards compatible. The main reasons are:

 - Performance, the database stores data in [bipf]
 - Replace flume with [jitdb] and specialized indexes

SSB-DB2 is a secret-stack plugin that registers itself in the db
namespace.

By default SSB-DB2 only loads a base index (indexes/base), this index
includes the basic functionality for getting messages from the log and
for doing EBT.

## Usage

To get the post messages of a specific author, you can do:

```js
const { fromDB, query, and, type, author, toCallback } = require('jitdb/operators')

const caps = require('ssb-caps')
const ssbKeys = require('ssb-keys')
const path = require('path')

const createSbot = require('secret-stack')({
    caps: { shs: Buffer.from(caps.shs, 'base64') },
})
      .use(require("ssb-db2"))

const dir = "./ssb"
var keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const sbot = createSbot({
  keys,
  path: dir,
})

query(
  sbot.db.jitdb,
  and(type('post')),
  and(author('@6CAxOI3f+LUOVrbAl0IemqiS7ATpQvr9Mdw9LC4+Uv0=.ed25519')),
  toCallback((err, msgs) => {
    console.log('There are ' + msgs.length + ' messages of type "post" from arj')
    sbot.close()
  })
)
```

A index plugin that exposes the most common used social aspects of SSB
is also available as indexes/social. It has 3 methods:

 - getMessagesByMention
 - getMessagesByRoot
 - getMessagesByVoteLink
 
This plugin is meant as a base for application developers to write
their own plugins if the functionality of jitdb is not enough. JITDB
is good for indexing specific values, like type `post`, whereas for
root messages where there are a lot of keys and only a few results for
each, a specialized index makes more sense.

To get the post messages of a specific root, you can do:

```js
const { fromDB, query, and, type, author, toCallback } = require('jitdb/operators')

const caps = require('ssb-caps')
const ssbKeys = require('ssb-keys')
const path = require('path')

const createSbot = require('secret-stack')({
    caps: { shs: Buffer.from(caps.shs, 'base64') },
})
      .use(require("ssb-db2"))
      .use(require("ssb-db2/social")) // include index

const dir = "./ssb"
var keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))

const sbot = createSbot({
  keys,
  path: dir,
})

sbot.db.indexes.social.getMessagesByRoot(msgKey, (err, rootMsgQuery) => {
  query(
    rootMsgQuery,
    and(type('post')),
    toCallback((err, msgs) => {
      console.log('There are ' + msgs.length + ')
      sbot.close() 
    })
  )
})
```

## Methods

FIXME: add documentation for these

[ssb-db]: https://github.com/ssbc/ssb-db/
[bipf]: https://github.com/ssbc/bipf/
[jitdb]: https://github.com/ssb-ngi-pointer/jitdb/
