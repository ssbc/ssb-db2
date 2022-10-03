const test = require('tape')
const ssbKeys = require('ssb-keys')
const rimraf = require('rimraf')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')

test('minimal db2 (no encryption)', t => {
  const dir = '/tmp/ssb-db2-minimal-create'
  rimraf.sync(dir)

  const keys = ssbKeys.generate()

  const stack = SecretStack({ appKey: caps.shs })
    .use(require('../core'))
    .use(require('ssb-classic'))
    // .use(require('ssb-box'))
    // .use(require('ssb-box2'))
    // .use(require('../compat/publish'))
    // .use(require('../compat/post'))
    // .use(require('../migrate'))

  const ssb = stack({
    path: dir,
    keys
  })

  ssb.db.create({
    content: { type: 'boop' },
    keys
  }, (err, msg) => {
    t.error(err, 'published message')

    ssb.close(true, t.end)
  })
})
