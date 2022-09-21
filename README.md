<!--
SPDX-FileCopyrightText: 2021 Anders Rune Jensen

SPDX-License-Identifier: CC0-1.0
-->

# SSB-DB2

SSB-DB2 is a new database for secure-scuttlebutt, it is meant as a
replacement for [ssb-db]. The main reason for creating a new database
is to be able to rework some of the existing decisions without having
to be 100% backwards compatible. The main reasons are:

- Performance, the database stores data in [bipf]
- Replace flume with [jitdb] and specialized indexes
- Run in the browser via [ssb-browser-core](https://github.com/arj03/ssb-browser-core)
- Work well with partial replication

Over time, this database received more features than ssb-db, and now supports:

- Deletion and compaction
- Customizable feed formats and encryption formats
  - You are not tied to classic SSB messages and the classic mode of encryption,
  you can use any format you want, or build one yourself, with [ssb-feed-format]
  and [ssb-encryption-format]
  - By default supports [ssb-classic]
- Query language (as composable JS functions)

SSB-DB2 is a secret-stack plugin that registers itself in the db
namespace.

By default SSB-DB2 only loads a base index (indexes/base), this index
includes the basic functionality for getting messages from the log and
for doing EBT.

By default the database is stored in `ssb/db2/log.bipf`, leveldb indexes
are stored in `ssb/db2/indexes/`, and jitdb indexes in `ssb/db2/jit`.

🎥 [Watch a presentation about this new database](https://www.youtube.com/watch?v=efzJheWQey8).

[Read the developer guide](https://dev.scuttlebutt.nz/#/javascript/?id=ssb-db2)

## Installation

- Requires **Node.js 12** or higher
- Requires `secret-stack@^6.2.0`

```diff
 SecretStack({appKey: require('ssb-caps').shs})
   .use(require('ssb-master'))
+  .use(require('ssb-db2'))
   .use(require('ssb-conn'))
   .use(require('ssb-blobs'))
   .call(null, config)
```

## Usage

To **create** and publish a new message, you can do:

```js
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')

const sbot = SecretStack({ caps })
  .use(require('ssb-db2'))
  .call(null, { path: './' })

sbot.db.create({ content: { type: 'post', text: 'hello!' } }, (err, msg) => {
  // A new message is now published on the log, with the contents above.
  console.log(msg)
  /*
  {
    key,
    value: {
      previous: null,
      sequence: 1,
      author,
      timestamp: 1633715006539,
      hash: 'sha256',
      content: { type: 'post', text: 'hello!' },
      signature,
    },
    timestamp: 1633715006540
  }
  */
})
```

To **get** the post messages of a specific author, you can do:

```js
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const { where, and, type, author, toCallback } = require('ssb-db2/operators')

const sbot = SecretStack({ caps })
  .use(require('ssb-db2'))
  .call(null, { path: './' })

sbot.db.query(
  where(
    and(
      type('post'),
      author('@6CAxOI3f+LUOVrbAl0IemqiS7ATpQvr9Mdw9LC4+Uv0=.ed25519')
    )
  ),
  toCallback((err, msgs) => {
    console.log(
      'There are ' + msgs.length + ' messages of type "post" from arj'
    )
    sbot.close()
  })
)
```

To get post messages that mention Alice, you can do:

```js
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const {where, and, type, mentions, toCallback} = require('ssb-db2/operators')

const sbot = SecretStack({ caps })
  .use(require('ssb-db2'))
  .call(null, { path: './' })

sbot.db.query(
  where(and(type('post'), mentions(alice.id)))),
  toCallback((err, msgs) => {
    console.log('There are ' + msgs.length + ' messages')
    sbot.close()
  })
)
```

### Leveldb plugins

The queries you've seen above use JITDB, but there are some queries
that cannot rely on JITDB alone, and we need to depend on
Leveldb. This section shows some example leveldb indexes, explains
when you need leveldb, and how to make your own leveldb plugin in
ssb-db2.

#### Full-mentions

An extra index plugin that is commonly needed in SSB communities is
the **full-mentions** index. It has one method: getMessagesByMention.

Although this accomplishes the same as the previous `mentions()`
example, this plugin is meant as an example for application developers
to write their own plugins if the functionality of JITDB is not
enough. JITDB is good for indexing specific values, like
`mentions(alice.id)` which gets its own dedidated JITDB index for
`alice.id`. But when querying mentions of several feeds or several
messages, this creates many indexes, so a specialized index makes more
sense.

What `full-mentions` does is index all possible mentionable items at
once, using Leveldb instead of JITDB. You can include it and use it
like this:

```js
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const {where, and, type, toCallback} = require('ssb-db2/operators')

const sbot = SecretStack({ caps })
  .use(require('ssb-db2'))
  .use(require('ssb-db2/full-mentions')) // include index
  .call(null, { path: './' })

const {fullMentions} = sbot.db.operators

sbot.db.query(
  where(and(type('post'), fullMentions(alice.id)))),
  toCallback((err, msgs) => {
    console.log('There are ' + msgs.length + ' messages')
    sbot.close()
  })
)
```

#### Your own leveldb index plugin

It's wise to use JITDB when:

1. You want the query output to be the msg itself, not state derived
   from msgs
2. You want the query output ordered by timestamp (either descending
   or ascending)

There are some cases where the assumptions above are not met. For
instance, with abouts, we often want to aggregate all `type: "about"`
msgs and return all recent values for each field (`name`, `image`,
`description`, etc). So assumption number 1 does not apply.

In that case, you can make a leveldb index for ssb-db2, by creating a
class that extends the class at `require('ssb-db2/indexes/plugin')`,
like this:

```js
const Plugin = require('ssb-db2/indexes/plugin')

// This is a secret-stack plugin
exports.init = function (sbot, config) {
  class MyIndex extends Plugin {
    constructor(log, dir) {
      //    log, dir, name, version, keyEncoding, valueEncoding
      super(log, dir, 'myindex', 1, 'utf8', 'json')
    }

    processRecord(record, seq) {
      const buf = record.value // this is a BIPF buffer, directly from the log
      // ...
      // Use BIPF seeking functions to decode some fields
      // ...
      this.batch.push({
        type: 'put',
        key: key, // some utf8 string here (see keyEncoding in the constructor)
        value: value, // some object here (see valueEncoding in the constructor)
      })
    }

    myOwnMethodToGetStuff(key, cb) {
      this.level.get(key, cb)
    }
  }

  sbot.db.registerIndex(MyIndex)
}
```

There are three parts you'll always need:

- `constructor`: here you set configurations for the Leveldb index
  - `log` and `dir` you probably don't need to fiddle with, but you
    can use `this.log` methods if you know how to use
    async-append-only-log
  - `name` is a string that you'll use in `getIndex(name)`, it's also
    used as a directory name
  - `version`, upon changing, will cause a full rebuild of this index
  - `keyEncoding` and `valueEncoding` must be strings from
    [level-codec]
- `processRecord`: here you handle a msg (in [bipf]) and potentially
  write something to the index using
  `this.batch.push(leveldbOperation)`
- **custom method**: this is an API of your own choosing, that allows
  you to read data from the index

To call your custom methods, you'll have to pick them like this:

```js
sbot.db.getIndex('myindex').myOwnMethodToGetStuff()
```

Or you can wrap that in a secret-stack plugin (in the example above,
`exports.init` should return an object with the API functions).

There are other optional methods you can implement in the `Plugin` subclass:

- `onLoaded(cb)`: a hook called once, at startup, when the index is successfully
  loaded from disk and is ready to receive queries
- `onFlush(cb)`: a hook called when the leveldb index is about to be saved to
  disk
- `indexesContent()`: method used when reindexing private group messages to
  determine if the leveldb index needs to be updated for decrypted messages. The
  default method returns true.
- `reset()`: a method that you can use to reset in-memory state that you might
  have in your plugin, when the leveldb index is about to be rebuilt.

### Compatibility plugins

SSB DB2 includes a couple of plugins for backwards compatibility,
including legacy replication, ebt and publish. They can be loaded as:

```js
const SecretStack = require('secret-stack')

const sbot = SecretStack({ caps })
  .use(require('ssb-db2'))
  .use(require('ssb-db2/compat')) // include all compatibility plugins
  .call(null, {})
```

or specifically:

```js
const SecretStack = require('secret-stack')

const sbot = SecretStack({ caps })
  .use(require('ssb-db2'))
  .use(require('ssb-db2/compat/db')) // basic db compatibility
  .use(require('ssb-db2/compat/log-stream')) // legacy replication
  .use(require('ssb-db2/compat/history-stream')) // legacy replication
  .use(require('ssb-db2/compat/ebt')) // ebt db helpers
  .use(require('ssb-db2/compat/publish')) // publish() function like in ssb-db
  .use(require('ssb-db2/compat/post')) // post() obv like in ssb-db
  .call(null, {})
```

## Secret-stack modules using ssb-db2

The following is a list of modules that works well with ssb-db2:

- [ssb-threads] for working with post messages as threads
- [ssb-suggest-lite] for fetching profiles of authors
- [ssb-friends] for working with the social graph
- [ssb-search2] for full-text searching
- [ssb-crut] for working with records that can be modified

## Migrating from ssb-db

The log used underneath ssb-db2 is different than that one in ssb-db,
this means we need to scan over the old log and copy all messages onto
the new log, if you wish to use ssb-db2 to make queries.

**⚠️ Warning: please read the following instructions** about using two
logs and carefully apply them to avoid forking feeds into an
irrecoverable state.

### Preventing forking feeds

The log is the source of truth in SSB, and now with ssb-db2, we
introduce a new log alongside the previous one. **One of them, not
both** has to be considered the source of truth.

While the old log exists, it will be continously migrated to the new
log, and ssb-db2 forbids you to use its database-writing APIs such as
`add()`, `publish()`, `del()` and so forth, to prevent the two logs
from diverging into inconsistent states. The old log will remain the
source of truth and the new log will just mirror it.

If you want to switch the source of truth to be the new log, we must
delete the old log, after it has been fully migrated. Only then can
you use database-writing APIs such as `publish()`. To delete the old
log, one method is to use the [config
`dangerouslyKillFlumeWhenMigrated`](#configuration). Set it to `true`
only when you are **absolutely sure** that no other app will attempt
to read/write to `~/.ssb/flume/log.offset` or wherever the old log
lives. It will delete the entire flume folder once migration has
completed writing the messages to the new log. From that point
onwards, using APIs such as `publish()` will succeed to append
messages to the new log.

### Triggering migration

ssb-db2 comes with migration methods built-in, you can enable them
(they are off by default!) in your config file (or object):

```js
const path = require('path')
const SecretStack = require('secret-stack')
const ssbKeys = require('ssb-keys')
const keys = ssbKeys.loadOrCreateSync(path.join(__dirname, 'secret'))

const config = {
  keys: keys,
  db2: {
    automigrate: true,
  },
}

const sbot = SecretStack({ caps })
  .use(require('ssb-db2'))
  .use(require('ssb-db2/compat'))
  .call(null, config)
```

The above script will initiate migration as soon as the plugins are
loaded. If you wish the manually dictate when the migration starts,
don't use the `automigrate` config above, instead, call the
`migrate.start()` method yourself:

```js
sbot.db.migrate.start()
```

Note, it is acceptable to load both ssb-db and ssb-db2 plugins, the
system will still function correctly and migrate correctly:

```js
const sbot = SecretStack({ caps })
  .use(require('ssb-db'))
  .use(require('ssb-db2'))
  .use(require('ssb-db2/compat'))
  .call(null, config)
```

### Migrating without including ssb-db2

Because ssb-db2 also begins indexing basic metadata once it's included
as a plugin, this may cost more (precious) CPU time. **If you are not
yet using db2 APIs** but would like to migrate the log anyway, in
preparation for later activating db2, then you can include only the
migration plugin, like this:

```js
const sbot = SecretStack({ appKey: caps.shs })
  .use(require('ssb-db2/migrate'))
  .call(null, config)
```

Note that the `start` behavior is the same: you can either start it
automatically using `config.db2.automigrate` or manually like this:

```js
sbot.db2migrate.start()
```

## Methods

### create(opts, cb)

Method for creating and publishing a new message on the log. The `opts` allows
you to fully customize how this message will be written, including which feed
format is used, which keys are signing/authoring the message, what encryption
format is used and so forth.

The `opts` must be an object and should contain the following keys:

- `content` **required**: the message content, MUST be an object and SHOULD
  include `type` as a string property.
- `feedFormat` _optional_ (defaults to `'classic'`): a string that specifies
  the feed format to use.
- `keys` _optional_ (defaults to `config.keys`): the keys to use for signing
  and authoring the message. `keys.id` must be valid for the `feedFormat` you
  selected.
- `encryptionFormat` _optional_, if you want to publish an encrypted message,
  it is recommended you set this field to a string that specifies which
  encryption format to use. Typically this is the string `'box'`.
- `recps`: _optional_, an array of feed IDs (strings) that will be used to
  encrypt the message. **The message is only encrypted if `opts.recps` (or
  `opts.content.recps`) exists**.
- `encoding`: _optional_ (defaults to `'js'`): a string that specifies the
  encoding to use when serializing the message to be written to the database.
  Supported values are `'js'` and `'bipf'`. Note that all messages in the
  database end up as `bipf` buffers even if you choose `'js'` encoding, so
  setting `encoding` to `'bipf'` is only a matter of improving serialize/persist
  performance **if** your selected feed format supports encoding to bipf
  directly.
- Depending on the feed format chosen, you may have to provide additional opts.
  See the docs for the specific feed format you are using.

The callback `cb` is called when the message has been published. `cb(err)` if
published failed with an error `err`, and `cb(null, kvt)` if it was
successfully published, where `kvt` is a JavaScript object with the shape
`{key, value, timestamp}` exactly representing the message written to the
database.

### get(msgId, cb)

Get a particular message value by message id.

### getMsg(msgId, cb)

Get a particular message including key by message id.

### query(...operators)

Flexible API to get messages from the database based on various criteria
determined by the `operators`. There are usually two parts in this list of
operators:

- The `where()` part, determining the criteria to match messages against.
  - Example: `where(and(type('post'), author(ssb.id)))`
- The `to____()` part, determining how you want the messages delivered, e.g.
  - `toCallback((err, msgs) => { })`
  - `toPromise()`
  - `toPullStream()`
  - `toAsyncIter()`

See [jitdb operators] and [operators/index.js] for a complete list of supported
operators.

### add(nativeMsg, cb)

Validate and add a message to the database. The callback will the (possible)
error in the 1st argument, and in the 2nd argument the stored message in "KVT"
shape, i.e. `{key, value, timestamp}`. The `nativeMsg` is assumed to belong to
a feed format that is currently installed, such as [ssb-classic].

**Alternatively:** `add(nativeMsg, opts, cb)` where `opts.encoding` and
`opts.feedFormat` can be specified. (Read above, in the `create` method, for
details about these opts)

### addOOO(nativeMsg, cb)

Validate and a message to the database, but validation will not check the
`previous`. Useful for partial replication.

Callback arguments are `(err, kvt)`, similar to the callback in the `add`
method.

**Alternatively:** `addOOO(nativeMsg, opts, cb)` where `opts.encoding` and
`opts.feedFormat` can be specified. (Read above, in the `create` method, for
details about these opts)

### addOOOBatch(nativeMsgs, cb)

Similar to `addOOO`, but you can pass an array of many messages. If the `author`
is not yet known, the message is validated without checking if the `previous`
link is correct, otherwise normal validation is performed. This makes it
possible to use for partial replication to add all contact messages from a feed.

Callback arguments are `(err, kvts)`, where `kvts` is an array of "KVT" matching
each of the given `nativeMsgs`.

**Alternatively:** `addOOOBatch(nativeMsgs, opts, cb)` where `opts.encoding` and
`opts.feedFormat` can be specified. (Read above, in the `create` method, for
details about these opts)

### addTransaction(nativeMsgs, oooNativeMsgs, cb)

Similar to `addOOOBatch`, except you pass in an array of `nativeMsgs` that will
be validated in order and an array of `oooNativeMsgs` that will be validated
similar to `addOOOBatch`. Finally all the messages are added to the database in
such a way that either all of them are written to disc or none of them are.

Callback arguments are `(err, kvts)`, similar to the callback in the
`addOOOBatch` method.

**Alternatively:** `addTransaction(nativeMsgs, oooNativeMsgs, opts, cb)` where
`opts.encoding` and `opts.feedFormat` can be specified. (Read above, in the
`create` method, for details about these opts)

### del(msgId, cb)

Delete a specific message given the message ID `msgId` from the database.
:warning: Please note that this will break replication for anything trying to
get that message, like `createHistoryStream` for the author or EBT. Because of
this, it is not recommended to delete message with this method unless you know
exactly what you are doing.

### deleteFeed(feedId, cb)

Delete all messages of a specific feedId. Compared to `del` this method is safe
to use.

### onMsgAdded(cb)

Subscribe to know when a message is added to the database. The `cb` will be
called as soon as a message is successfully persisted to the log, with one
argument, `event`, which contains:

- `event.feedFormat`: the feed format used to persist the message.
- `event.nativeMsg`: the message that was just added to the database, in the
  "native" shape determined by its feed format. This could be e.g. a buffer.
- `event.kvt`: the message that was just added to the database, in the
  `{key, value, timestamp}` shape, as a JavaScript object.

`onMsgAdded` itself is an [obz], so the latest message added to the database can
also be read using `ssb.db.onMsgAdded.value`.

### getStatus

Gets the current db status, same functionality as
[db.status](https://github.com/ssbc/ssb-db#dbstatus) in ssb-db.

### reindexEncrypted(cb)

This function is useful in [ssb-box2] where box2 keys can be added
at runtime and that changes what messages can be decrypted. Calling
this function is needed after adding a new key. The function can be
called multiple times safely.

### logStats(cb)

Use [async-append-only-log]'s `stats` method to get information on how many
bytes are used by messages in the log, and how many bytes are zero-filled.

### prepare(operation, cb)

Use [JITDB's prepare](https://github.com/ssb-ngi-pointer/jitdb/#prepareoperation-cb) method to warm up a JIT index.

### onDrain(indexName?, cb)

Waits for the index with name `indexName` to be in sync with the main
log and then call `cb` with no arguments. If `indexName` is not
provided, the base index will be used.

The reason we do it this way is that indexes are updated
asynchronously in order to not block message writing.

### compact(cb)

Compacts the log (filling in the blanks left by deleted messages and optimizing
space) and then rebuilds indexes.

### installFeedFormat(feedFormat)

If `feedFormat` conforms to the [ssb-feed-format] spec, then this method will
install the `feedFormat` in this database instance, meaning that you can create
messages for that feed format using the `create` method.

### installEncryptionFormat(encryptionFormat)

If `encryptionFormat` conforms to the [ssb-encryption-format] spec, then this
method will install the `encryptionFormat` in this database instance, meaning
that you can now encrypt and decrypt messages using that encryption format.

### reset(cb)

Force all indexese to be rebuilt. Use this as a last resort if you suspect that
the indexes are corrupted.

## Configuration

You can use ssb-config parameters to configure some aspects of ssb-db2:

```js
const config = {
  keys: keys,
  db2: {
    /**
     * Start the migration plugin automatically as soon as possible.
     * Default: false
     */
    automigrate: true,

    /**
     * If the migration plugin is used, then when migration has completed, we
     * will remove the entire `~/.ssb/flume` directory, including the log.
     *
     * As the name indicates, this is dangerous, because if there are other apps
     * that still use `~/.ssb/flume`, they will see an empty log and progress to
     * write on that empty log using the `~/.ssb/secret` and this will very
     * likely fork the feed in comparison to new posts on the new log. Only use
     * this when you know the risks and you know that only the new log will be
     * written.
     * Default: false
     */
    dangerouslyKillFlumeWhenMigrated: false,

    /**
     * Only try to decrypt box1 messages created after this date
     * Default: null
     */
    startDecryptBox1: '2022-03-25',

    /**
     * A throttle interval (measured in milliseconds) to control how often
     * should messages given to `sbot.add` be flushed in batches.
     * Default: 250
     */
    addBatchThrottle: 250,

    /**
     * An upper limit on the CPU load that ssb-db2 can use while indexing
     * and scanning. `85` means "ssb-db2 will only index when CPU load is at
     * 85% or lower".
     * Default: Infinity
     */
    maxCpu: 85,

    /** This applies only if `maxCpu` is defined.
     * See `maxPause` in the module `too-hot`, for its definition.
     * Default: 300
     */
    maxCpuMaxPause: 180,

    /** This applies only if `maxCpu` is defined.
     * See `wait` in the module `too-hot`, for its definition.
     * Default: 90
     */
    maxCpuWait: 90,
  },
}
```

## Operators

The following operators are included by default, see
[operators/index.js] for how they are implemented. Also exposed are
all [jitdb operators]

- type
- author
- channel
- key
- votesFor
- contact
- mentions
- about
- hasRoot
- hasFork
- hasBranch
- authorIsBendyButtV1
- isRoot
- isPublic
- isEncrypted
- isDecrypted

[ssb-db]: https://github.com/ssbc/ssb-db/
[async-append-only-log]: https://github.com/ssbc/async-append-only-log/
[bipf]: https://github.com/ssbc/bipf/
[jitdb]: https://github.com/ssb-ngi-pointer/jitdb/
[bendy butt]: https://github.com/ssb-ngi-pointer/ssb-bendy-butt
[obz]: https://github.com/ssbc/obz/
[ssb-social-index]: https://github.com/ssbc/ssb-social-index
[ssb-box2]: https://github.com/ssb-ngi-pointer/ssb-box2
[level-codec]: https://github.com/Level/codec#builtin-encodings
[ssb-threads]: https://github.com/ssbc/ssb-threads
[ssb-suggest-lite]: https://github.com/ssb-ngi-pointer/ssb-suggest-lite
[ssb-friends]: https://github.com/ssbc/ssb-friends
[ssb-classic]: https://github.com/ssbc/ssb-classic
[ssb-feed-format]: https://github.com/ssbc/ssb-feed-format
[ssb-encryption-format]: https://github.com/ssbc/ssb-encryption-format
[ssb-search2]: https://github.com/staltz/ssb-search2
[ssb-crut]: https://gitlab.com/ahau/lib/ssb-crut
[operators/index.js]: https://github.com/ssb-ngi-pointer/ssb-db2/blob/master/operators/index.js
[jitdb operators]: https://github.com/ssb-ngi-pointer/jitdb#operators
