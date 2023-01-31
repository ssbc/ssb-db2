<!--
SPDX-FileCopyrightText: 2022 Andre 'Staltz' Medeiros

SPDX-License-Identifier: CC0-1.0
-->

## 7.0.0

### Breaking changes

- Node.js 16 or higher required, lower versions will not work

## 6.0.0

### Breaking changes

- `config.db2.addDebounce` replaced with `config.db2.addBatchThrottle`

## 5.2.0

### New APIs

- `logStats()`

## 5.1.0

### New APIs

- `isPrivate()` operator

We brought back `isPrivate()` which is just a shortcut operator that does the
same as `or(isEncryted(), isDecrypted())`.

## 5.0.0

### New APIs

- `create()`
- `onMsgAdded()`
- `installFeedFormat()`
- `installEncryptionFormat()`

Check the README for more details on these APIs.

### Breaking changes

- The operator `isPrivate()` removed in favor of two new operators: `isDecrypted()` and `isEncrypted()`, both support the encryption format name as an optional argument, e.g. `isDecrypted('box')` and `isEncrypted('box2')`
- `ssb.db.publish()` no longer supports box2 recipients such as private-groups. You need to use `ssb.db.create()` for that.

### Notable changes

- The indexes `canDecrypt.index` and `encrypted.index` were renamed to `decrypted.index` and `encrypted-box2.index` respectively. You can either let ssb-db2 recreate these indexes with the new names, or you can manually rename them *before* ssb-db2 loads.
- Deprecated APIs: `ssb.db.post`, `ssb.db.publish`, `ssb.db.publishAs` are marked as obsolete, in favor of `onMsgAdded` and `create()`, but still supported as before. In a future version they may be removed from the default setup and you'll have to manually import them from `ssb-db2/compat/post` and `ssb-db2/compat/publish`.

## 4.0.0

### Breaking changes

- JITDB indexes are now stored in `db2/jit` while previously they were stored in `db2/indexes`. Updating ssb-db2 from 3.0.0 to 4.0.0 requires no changes in your code, but if you want to avoid the JITDB indexes being rebuilt from scratch, then you'll have to move all `*.32prefix`, `*.32prefixmap`, and `*.index` files (**except** `canDecrypt.index` and `encrypted.index`) from `db2/indexes/` to `db2/jit/`.

## 3.0.0

### Breaking changes

- Previously, ssb-db2 emitted events on the secret-stack event emitter with event names `ssb:db2:indexing:progress` and `ssb:db2:migrate:progress`. From version 3.0.0 those have been replaced by conventional muxrpc `source` APIs at `sbot.db2.indexingProgress()` and `sbot.db2migrate.progress()`, respectively.

## 2.0.0

### Breaking changes

- ssb-db2 now uses `jitdb@3` with the new `where()` operator. All your queries will have to be updated to use `where()`, even though it's straightforward to adopt this new operator.
