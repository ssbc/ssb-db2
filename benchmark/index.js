const test = require('tape')
const fs = require('fs')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const generateFixture = require('ssb-fixtures')
const SecretStack = require('secret-stack')
const caps = require('ssb-caps')
const ssbKeys = require('ssb-keys')
const multicb = require('multicb')
const pull = require('pull-stream')
const fromEvent = require('pull-stream-util/from-event')
const DeferredPromise = require('p-defer')
const sleep = require('util').promisify(setTimeout)
const {
  and,
  type,
  author,
  key,
  votesFor,
  isPublic,
  isRoot,
  hasRoot,
  startFrom,
  paginate,
  descending,
  toCallback,
} = require('../operators')

const dir = '/tmp/ssb-db2-benchmark'
const oldLogPath = path.join(dir, 'flume', 'log.offset')
const db2Path = path.join(dir, 'db2')
const reportPath = path.join(dir, 'benchmark.md')

const skipCreate = process.argv[2] === 'noCreate'

if (!skipCreate) {
  rimraf.sync(dir)
  mkdirp.sync(dir)

  const SEED = 'sloop'
  const MESSAGES = 100000
  const AUTHORS = 2000

  test('generate fixture with flumelog-offset', (t) => {
    generateFixture({
      outputDir: dir,
      seed: SEED,
      messages: MESSAGES,
      authors: AUTHORS,
      slim: true,
    }).then(() => {
      t.pass(`seed = ${SEED}`)
      t.pass(`messages = ${MESSAGES}`)
      t.pass(`authors = ${AUTHORS}`)
      t.true(fs.existsSync(oldLogPath), 'log.offset was created')
      fs.appendFileSync(reportPath, '## Benchmark results\n\n')
      fs.appendFileSync(reportPath, '| Part | Duration |\n|---|---|\n')
      t.end()
    })
  })
}

let maxRAMrss = 0
let maxRAMheap = 0
function updateMaxRAM() {
  const memUsage = process.memoryUsage()
  maxRAMrss = Math.max(maxRAMrss, memUsage.rss)
  maxRAMheap = Math.max(maxRAMheap, memUsage.heapUsed)
}

function toMB(bytes) {
  return (bytes / 1000 / 1000).toFixed(2)
}

function reportMem() {
  const rss = toMB(maxRAMrss)
  const heap = toMB(maxRAMheap)
  return `${rss} MB = ${heap} MB + etc`
}

test('migration (using ssb-db)', async (t) => {
  rimraf.sync(db2Path)
  t.pass('delete db2 folder to start clean')

  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('ssb-db'))
    .use(require('../migrate'))
    .call(null, { keys, path: dir })

  while (true) {
    const { current, target } = sbot.progress().indexes
    if (current === target) break
    else await sleep(500)
  }
  t.pass('ssb-db has finished indexing')

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()
  const start = Date.now()
  sbot.db2migrate.start()

  pull(
    fromEvent('ssb:db2:migrate:progress', sbot),
    pull.filter((progress) => progress === 1),
    pull.take(1),
    pull.drain(async () => {
      const duration = Date.now() - start
      t.pass(`duration: ${duration}ms`)
      fs.appendFileSync(
        reportPath,
        `| Migration (using ssb-db) | ${duration}ms |\n`
      )
      updateMaxRAM()
      global.gc()
      await sleep(2000) // wait for new log FS writes to finalize
      sbot.close(() => {
        ended.resolve()
      })
    })
  )

  await ended.promise
})

test('migration (alone)', async (t) => {
  rimraf.sync(db2Path)
  t.pass('delete db2 folder to start clean')

  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../migrate'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()
  const start = Date.now()
  sbot.db2migrate.start()

  pull(
    fromEvent('ssb:db2:migrate:progress', sbot),
    pull.filter((progress) => progress === 1),
    pull.take(1),
    pull.drain(async () => {
      const duration = Date.now() - start
      t.pass(`duration: ${duration}ms`)
      fs.appendFileSync(reportPath, `| Migration (alone) | ${duration}ms |\n`)
      updateMaxRAM()
      global.gc()
      t.pass(`memory usage without indexes: ${reportMem()}`)
      fs.appendFileSync(
        reportPath,
        `| Memory usage without indexes | ${reportMem()} |\n`
      )
      await sleep(2000) // wait for new log FS writes to finalize
      sbot.close(() => {
        ended.resolve()
      })
    })
  )

  await ended.promise
})

test('initial indexing', async (t) => {
  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()
  const start = Date.now()

  sbot.db.query(
    and(type('post')),
    descending(),
    paginate(1),
    toCallback((err, { results, total }) => {
      const duration = Date.now() - start
      t.error(err)
      if (total === 0) t.fail('should respond with msgs')
      if (results.length !== 1) t.fail('should respond with 1 msg')
      if (!results[0].value.content.text.includes('LATESTMSG'))
        t.fail('should have LATESTMSG')
      t.pass(`duration: ${duration}ms`)
      fs.appendFileSync(reportPath, `| Initial indexing | ${duration}ms |\n`)
      updateMaxRAM()
      global.gc()
      sbot.close(() => {
        ended.resolve()
      })
    })
  )

  await ended.promise
})

test('initial indexing compat', async (t) => {
  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .use(require('../compat'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()
  const start = Date.now()

  sbot.db.onDrain('base', () => {
    sbot.db.onDrain('ebt', () => {
      const duration = Date.now() - start
      t.pass(`duration: ${duration}ms`)
      fs.appendFileSync(
        reportPath,
        `| Initial indexing compat | ${duration}ms |\n`
      )
      updateMaxRAM()
      global.gc()
      sbot.close(() => {
        ended.resolve()
      })
    })
  })

  await ended.promise
})

test('Two indexes updating concurrently', async (t) => {
  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .use(require('../compat'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()
  const done = multicb({ pluck: 1 })
  const start = Date.now()

  sbot.db.query(and(type('about')), toCallback(done()))
  sbot.db.query(and(type('about'), isPublic()), toCallback(done()))

  done((err) => {
    if (err) t.fail(err)
    const duration = Date.now() - start
    t.pass(`duration: ${duration}ms`)
    fs.appendFileSync(
      reportPath,
      `| Two indexes updating concurrently | ${duration}ms |\n`
    )
    updateMaxRAM()
    global.gc()
    sbot.close(() => ended.resolve())
  })

  await ended.promise
})

const KEY1 = '%Xwdpu9gRe8wl4i0ssKyU24oGYKXW75hjE5VCbEB9bmM=.sha25' // root post
const KEY2 = '%EpzOw6sOBb4RGtofVD43GnfImoiw6NzEEsraHsNXF1g=.sha25' // contact
const KEY3 = '%55wBq68+p45q7/OuPgL+TC07Ifx8ihEW93u/EZaYv6c=.sha256' // another post
const AUTHOR1 = '@ZngOKXHjrvG+cy7Gjx5pSFunUqcePfmDQQxoUlHFUdU=.ed2551'
const AUTHOR2 = '@58u/J9+5bOXeYRDCYQ9cJ7kklghIpQFPBYxlhKq1/qs=.ed2551'

const queries = {
  'key one initial': [and(key(KEY1))],

  'key two': [and(key(KEY2))],

  'key one again': [and(key(KEY1))],

  'latest root posts': [
    and(type('post'), isRoot(), isPublic()),
    startFrom(0),
    paginate(25),
    descending(),
  ],

  'latest posts': [
    and(type('post'), isPublic()),
    startFrom(0),
    paginate(25),
    descending(),
  ],

  'votes one initial': [and(votesFor(KEY1))],

  'votes again': [and(votesFor(KEY3))],

  hasRoot: [and(hasRoot(KEY1))],

  'hasRoot again': [and(hasRoot(KEY3))],

  'author one posts': [
    and(type('post'), author(AUTHOR1), isPublic()),
    startFrom(0),
    paginate(25),
    descending(),
  ],

  'author two posts': [
    and(type('post'), author(AUTHOR2), isPublic()),
    startFrom(0),
    paginate(25),
    descending(),
  ],

  'dedicated author one posts': [
    and(type('post'), author(AUTHOR1, { dedicated: true }), isPublic()),
    startFrom(0),
    paginate(25),
    descending(),
  ],

  'dedicated author one posts again': [
    and(type('post'), author(AUTHOR1, { dedicated: true }), isPublic()),
    startFrom(0),
    paginate(25),
    descending(),
  ],
}

let sbot
test('setup', (t) => {
  const keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, { keys, path: dir })
  t.end()
})

for (const title in queries) {
  test(title, (t) => {
    const start = Date.now()

    sbot.db.query(
      ...queries[title],
      toCallback((err) => {
        if (err) t.fail(err)
        const duration = Date.now() - start
        t.pass(`duration: ${duration}ms`)
        fs.appendFileSync(reportPath, `| ${title} | ${duration}ms |\n`)
        updateMaxRAM()
        t.end()
      })
    )
  })
}

test('maximum RAM used', (t) => {
  t.pass(`maximum memory usage: ${reportMem()}`)
  fs.appendFileSync(reportPath, `| Maximum memory usage | ${reportMem()} |\n`)
  t.end()
})

test('teardown', (t) => {
  sbot.close(t.end)
})
