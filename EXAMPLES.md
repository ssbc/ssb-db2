# Example

This page shows some examples of converting from ssb-db (and its various helper modules, ssb-backlinks, ssb-query, etc) to ssb-db2.

## Live stream of vote messages

Includes also old messages.

### Before

```js
pull(
  sbot.backlinks.read({
    query: [{ $filter: { dest: msgId } }],
    index: 'DTA',
    live: true
  }),
  pull.drain(msg => {
    // ...
  })
)
```

### After

```js
const {and, votesFor, live, toPullStream} = require('ssb-db2/operators')

pull(
  sbot.db.query(
    and(votesFor(msgId)),
    live({ old: true }),
    toPullStream()
  ),
  pull.drain(msg => {
    // ...
  })
)
```

## Get latest contact message from Alice about Bob

### Before

```js
pull(
  sbot.links({
    source: aliceId,
    dest: bobId,
    rel: 'contact',
    live: false,
    reverse: true
  }),
  pull.take(1),
  pull.drain(msg => {
    // ...
  })
)
```

### After

```js
const {and, author, contact, descending, paginate, toCallback} =
  require('ssb-db2/operators')

sbot.db.query(
  and(author(aliceId), contact(bobId)),
  descending(),
  paginate(1),
  toCallback((err, response) => {
    const msg = response.results[0]
    // ...
  })
)
```
