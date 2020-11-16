const test = require('tape');
const ssbKeys = require('ssb-keys');
const path = require('path');
const rimraf = require('rimraf');
const mkdirp = require('mkdirp');
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const generateFixture = require('ssb-fixtures');
const fs = require('fs');
const DB = require('../db');
const migration = require('../migration');
const {toCallback} = require('../operators')

const dir = '/tmp/ssb-db2-migration';

rimraf.sync(dir);
mkdirp.sync(dir);

let keys;
let db;

test('generate fixture with flumelog-offset', (t) => {
  generateFixture({
    outputDir: dir,
    seed: 'migration',
    messages: 100,
    authors: 5,
    slim: true,
  }).then(() => {
    t.true(
      fs.existsSync(path.join(dir, 'flume', 'log.offset')),
      'log.offset was created',
    );
    t.end();
  });
});

test('migration creates ~/.ssb/flumelog.bipf', (t) => {
  const config = {
    path: dir,
    _db2migrationCB: () => {
      t.true(
        fs.existsSync(path.join(dir, 'db2', 'log.bipf')),
        'migration done',
      );
      t.end();
    },
  };
  migration.init(null, config);
});

test('migrated db can be used with ssb-db2 APIs', (t) => {
  keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  db = DB.init(dir, {
    path: dir,
    keys
  })
  t.equals(typeof db.jitdb, 'object')
  db.onDrain(() => {
    db.query(
      toCallback((err1, msgs) => {
        t.error(err1, 'no err')
        t.equal(msgs.length, 100)
        t.end()
      })
    )
  })
})

// FIXME:
test.skip('migrated db stays up-to-date as the old log is updated', (t) => {
  db.onDrain(() => {
    db.query(
      toCallback((err1, msgs) => {
        t.error(err1, 'no err')
        t.equal(msgs.length, 100)

        const sbot = SecretStack({appKey: caps.shs})
          .use(require('ssb-db'))
          .call(null, {keys, path: dir})
        sbot.publish({ type: 'post', text: 'Extra post' }, (err2, posted) => {
          t.error(err2, 'no err')
          t.equals(posted.value.content.type, 'post')

          db.onDrain(() => {
            db.query(
              toCallback((err3, msgs2) => {
                t.error(err3, 'no err')
                t.equal(msgs2.length, 101)
                t.end()
              })
            )
          })
        })
      })
    )
  })
})