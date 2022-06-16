// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: Unlicense

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
const classic = require('ssb-classic/format')
const butt2 = require('ssb-buttwoo')
const bipf = require('bipf')
const DeferredPromise = require('p-defer')
const trammel = require('trammel')
const sleep = require('util').promisify(setTimeout)
const {
  and,
  where,
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
const dirAdd = '/tmp/ssb-db2-benchmark-add'
const dirBox1 = '/tmp/ssb-db2-benchmark-box1'
const dirBox1NoDecrypt = '/tmp/ssb-db2-benchmark-box1-no-decrypt'
const dirBox2 = '/tmp/ssb-db2-benchmark-box2'
const oldLogPath = path.join(dir, 'flume', 'log.offset')
const db2Path = path.join(dir, 'db2')
const indexesPath = path.join(dir, 'db2', 'indexes')
const reportPath = path.join(dir, 'benchmark.md')

rimraf.sync(dirAdd)
rimraf.sync(dirBox1)
rimraf.sync(dirBox1NoDecrypt)
rimraf.sync(dirBox2)

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

function capitalize(str) {
  return str.charAt(0).toUpperCase() + str.slice(1)
}

const measurements = new Map()
function startMeasure(t, name) {
  if (measurements.has(name)) {
    t.fail(`Measurement ${name} already started`)
  } else {
    measurements.set(name, performance.now())
  }
}

function endMeasure(t, name) {
  if (measurements.has(name)) {
    const start = measurements.get(name)
    const duration = performance.now() - start
    t.pass(`${name}: ${duration.toFixed(2)}ms`)
    fs.appendFileSync(
      reportPath,
      `| ${capitalize(name)} | ${duration.toFixed(2)}ms |\n`
    )
    measurements.delete(name)
  } else {
    t.fail(`Measurement ${name} not started`)
  }
}

let keys, keys2, keys3, keys4
test('setup', (t) => {
  keys = ssbKeys.loadOrCreateSync(path.join(dir, 'secret'))
  keys2 = ssbKeys.loadOrCreateSync(path.join(dirBox1, 'secret'))
  keys3 = ssbKeys.loadOrCreateSync(path.join(dirBox1NoDecrypt, 'secret'))
  keys4 = ssbKeys.loadOrCreateSync(path.join(dirBox2, 'secret'))
  t.end()
})

test('buttwoo testing', (t) => {
  rimraf.sync(db2Path)
  t.pass('delete db2 folder to start clean')

  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, { keys, path: dirAdd })

  const butt2Key = ssbKeys.generate(null, null, 'buttwoo-v1')
  const format = sbot.db.findFeedFormatByName('buttwoo-v1')

  const N = 5 * 1000
  const content = { text: 'hello world', type: 'post' }

  let messages = []
  let sbotMessages = []
  const hmac = null
  let previousBFE = null
  let previousBFESbot = null
  let startDate = +new Date()

  startMeasure(t, `create ${N} new messages`)
  for (let i = 0; i < N; ++i) {
    const [msgKeyBFE, butt2Msg] = butt2.encodeNew(
      content,
      butt2Key,
      null,
      messages.length + 1,
      previousBFE,
      startDate++,
      butt2.tags.SSB_FEED,
      hmac
    )
    messages.push(butt2Msg)
    previousBFE = msgKeyBFE

    const sbotButt2Msg = format.newNativeMsg({
      keys: butt2Key,
      previous: {
        key: previousBFESbot,
        value: { sequence: sbotMessages.length },
      },
      content,
      tag: 0,
      timestamp: startDate++,
    })
    previousBFESbot = format.getMsgId(sbotButt2Msg)
    sbotMessages.push(sbotButt2Msg)
  }
  endMeasure(t, `create ${N} new messages`)

  const hmacKey = null
  const msgKeys = []
  const extractedData = []

  startMeasure(t, `validate ${N} messages ssb-buttwoo`)
  for (let i = 0; i < N; ++i) {
    const msg = messages[i]
    const e = butt2.extractData(msg)
    extractedData.push(e)
    const msgKeyBFE = butt2.hash(e)
    msgKeys.push(msgKeyBFE)
  }

  let isOk = true

  for (let i = 0; i < N; ++i) {
    const prevData = i === 0 ? null : extractedData[i - 1]
    const prevMsgKey = i === 0 ? null : msgKeys[i - 1]

    const validate = butt2.validateSingle(
      extractedData[i],
      prevData,
      prevMsgKey,
      hmacKey
    )
    if (typeof validate === 'string') {
      isOk = false
      break
    }
  }
  endMeasure(t, `validate ${N} messages ssb-buttwoo`)

  if (!isOk) console.log('failed validation')

  startMeasure(t, `validate ${N} messages sbot`)
  for (let i = 0; i < N; ++i) {
    const prev = i === 0 ? null : sbotMessages[i - 1]
    format.validate(sbotMessages[i], prev, hmacKey, (err) => {
      if (err) console.log(err)
    })
  }
  endMeasure(t, `validate ${N} messages sbot`)

  const bipfs = []
  const bipfsSbot = []

  startMeasure(t, `native to db format ${N} messages ssb-buttwoo`)
  for (let i = 0; i < N; ++i) {
    const dbFormat = butt2.butt2ToBipf(extractedData[i], msgKeys[i])
    bipfs.push(dbFormat)
  }
  endMeasure(t, `native to db format ${N} messages ssb-buttwoo`)

  startMeasure(t, `native to db format ${N} messages sbot`)
  for (let i = 0; i < N; ++i) {
    const value = format.fromNativeMsg(sbotMessages[i], 'bipf')
    const key = format.getMsgId(sbotMessages[i])

    bipf.markIdempotent(value)
    const kvt = {
      key,
      value,
      timestamp: Date.now(),
    }
    const recBuffer = bipf.allocAndEncode(kvt)

    bipfsSbot.push(recBuffer)
  }
  endMeasure(t, `native to db format ${N} messages sbot`)

  startMeasure(t, `db to native format ${N} messages ssb-buttwoo`)
  for (let i = 0; i < N; ++i) {
    const dbFormat = butt2.bipfToButt2(bipfs[i])
  }
  endMeasure(t, `db to native format ${N} messages ssb-buttwoo`)

  const BIPF_AUTHOR = bipf.allocAndEncode('author')
  const BIPF_VALUE = bipf.allocAndEncode('value')

  startMeasure(t, `db to native format ${N} messages sbot`)
  for (let i = 0; i < N; ++i) {
    const buffer = bipfsSbot[i]

    const pValue = bipf.seekKey2(buffer, 0, BIPF_VALUE, 0)

    const feedFormat = sbot.db.findFeedFormatByName('buttwoo-v1')

    let nativeMsg
    if (feedFormat.encodings.includes('bipf')) {
      const valueBuf = bipf.pluck(buffer, pValue)
      nativeMsg = feedFormat.toNativeMsg(valueBuf, 'bipf')
    } else {
      const msgVal = bipf.decode(buffer, pValue)
      nativeMsg = feedFormat.toNativeMsg(msgVal, 'js')
    }
  }
  endMeasure(t, `db to native format ${N} messages sbot`)

  sbot.close(true, t.end)
})

test('add a bunch of messages', (t) => {
  rimraf.sync(db2Path)
  t.pass('delete db2 folder to start clean')

  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, { keys, path: dirAdd })

  let previous = null
  const msgVals = []
  for (var i = 0; i < 1000; ++i) {
    const msgVal = classic.newNativeMsg({
      keys,
      content: {type:'tick', count: i},
      previous,
      timestamp: Date.now(),
      hmacKey: null,
    })
    msgVals.push(msgVal)
    previous = {key : classic.getMsgId(msgVal), value: msgVal}
  }

  const done = multicb({ pluck: 1 })
  startMeasure(t, 'add 1000 elements')
  for (const msgVal of msgVals) {
    sbot.db.add(msgVal, done())
  }
  done((err) => {
    endMeasure(t, 'add 1000 elements')
    if (err) t.fail(err)

    sbot.close(true, t.end)
  })
})

const randos = [
  ssbKeys.generate().id,
  ssbKeys.generate().id,
  ssbKeys.generate().id,
  ssbKeys.generate().id,
  ssbKeys.generate().id,
]

function shuffle(a) {
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1))
    ;[a[i], a[j]] = [a[j], a[i]]
  }
  return a
}

test('box1', (t) => {
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, { keys: keys2, path: dirBox1 })

  const recps = [...randos, keys.id]

  let contents = []
  for (var i = 0; i < 1000; ++i)
    contents.push({ type: 'tick', count: i, recps: shuffle(recps) })

  startMeasure(t, 'add 1000 box1 msgs')
  pull(
    pull.values(contents),
    pull.asyncMap(sbot.db.publish),
    pull.collect((err, msgs) => {
      endMeasure(t, 'add 1000 box1 msgs')
      if (err) t.fail(err)

      sbot.db.onDrain('base', () => {
        startMeasure(t, 'unbox 1000 box1 msgs first run')

        sbot.db.query(
          where(author(sbot.id)),
          toCallback((err, results) => {
            endMeasure(t, 'unbox 1000 box1 msgs first run')

            startMeasure(t, 'unbox 1000 box1 msgs second run')
            sbot.db.query(
              where(author(sbot.id)),
              toCallback((err, results) => {
                endMeasure(t, 'unbox 1000 box1 msgs second run')

                sbot.close(true, t.end)
              })
            )
          })
        )
      })
    })
  )
})

test('private box1 no decrypt', (t) => {
  const startFrom = new Date()
  startFrom.setDate(startFrom.getDate() + 1)
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, {
      keys: keys3,
      path: dirBox1NoDecrypt,
      db2: { startDecryptBox1: startFrom.toISOString().split('T')[0] },
    })

  const recps = [...randos, keys.id]

  let contents = []
  for (var i = 0; i < 1000; ++i)
    contents.push({ type: 'tick', count: i, recps: shuffle(recps) })

  startMeasure(t, 'add 1000 box1 msgs')
  pull(
    pull.values(contents),
    pull.asyncMap(sbot.db.publish),
    pull.collect((err, msgs) => {
      endMeasure(t, 'add 1000 box1 msgs')
      if (err) t.fail(err)

      sbot.db.onDrain('base', () => {
        startMeasure(t, 'query 1000 msgs first run')
        sbot.db.query(
          where(author(sbot.id)),
          toCallback((err, results) => {
            endMeasure(t, 'query 1000 msgs first run')

            startMeasure(t, 'query 1000 msgs second run')
            sbot.db.query(
              where(author(sbot.id)),
              toCallback((err, results) => {
                endMeasure(t, 'query 1000 msgs second run')

                sbot.close(true, t.end)
              })
            )
          })
        )
      })
    })
  )
})

test('private box2', (t) => {
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, {
      keys: keys4,
      path: dirBox2,
      box2: {
        alwaysbox2: true,
      },
    })

  const recps = [...randos, keys2.id]

  const testkey = Buffer.from(
    '30720d8f9cbf37f6d7062826f6decac93e308060a8aaaa77e6a4747f40ee1a76',
    'hex'
  )
  sbot.box2.addOwnDMKey(testkey)

  let contents = []
  for (var i = 0; i < 1000; ++i)
    contents.push({ type: 'tick', count: i, recps: shuffle(recps) })

  startMeasure(t, 'add 1000 box2 msgs')
  pull(
    pull.values(contents),
    pull.asyncMap(sbot.db.publish),
    pull.collect((err, msgs) => {
      endMeasure(t, 'add 1000 box2 msgs')
      if (err) t.fail(err)

      sbot.db.onDrain('base', () => {
        startMeasure(t, 'unbox 1000 box2 msgs first run')
        sbot.db.query(
          where(author(sbot.id)),
          toCallback((err, results) => {
            endMeasure(t, 'unbox 1000 box2 msgs first run')

            startMeasure(t, 'unbox 1000 box2 msgs second run')
            sbot.db.query(
              where(author(sbot.id)),
              toCallback((err, results) => {
                endMeasure(t, 'unbox 1000 box2 msgs second run')

                sbot.close(true, t.end)
              })
            )
          })
        )
      })
    })
  )
})

test('migrate (+db1)', async (t) => {
  rimraf.sync(db2Path)
  t.pass('delete db2 folder to start clean')

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
  startMeasure(t, 'migrate (+db1)')
  sbot.db2migrate.start()

  pull(
    sbot.db2migrate.progress(),
    pull.filter((progress) => {
      console.log(progress);
      return progress === 1
    }),
    pull.take(1),
    pull.drain(async () => {
      endMeasure(t, 'migrate (+db1)')
      await sleep(2000) // wait for new log FS writes to finalize
      sbot.close(true, () => ended.resolve())
    })
  )

  await ended.promise
})

test('migrate (alone)', async (t) => {
  rimraf.sync(db2Path)
  t.pass('delete db2 folder to start clean')

  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../migrate'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()
  startMeasure(t, 'migrate (alone)')
  sbot.db2migrate.start()

  pull(
    sbot.db2migrate.progress(),
    pull.filter((progress) => progress === 1),
    pull.take(1),
    pull.drain(async () => {
      endMeasure(t, 'migrate (alone)')
      await sleep(2000) // wait for new log FS writes to finalize
      sbot.close(true, () => ended.resolve())
    })
  )

  await ended.promise
})

test('migrate (+db1 +db2)', async (t) => {
  rimraf.sync(db2Path)
  t.pass('delete db2 folder to start clean')

  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('ssb-db'))
    .use(require('../'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()
  startMeasure(t, 'migrate (+db1 +db2)')
  sbot.db2migrate.start()

  pull(
    sbot.db2migrate.progress(),
    pull.filter((progress) => progress === 1),
    pull.take(1),
    pull.drain(async () => {
      endMeasure(t, 'migrate (+db1 +db2)')
      await new Promise((resolve) => sbot.db.onDrain(resolve))
      sbot.close(true, () => ended.resolve())
    })
  )

  await ended.promise
})

test('migrate (+db2)', async (t) => {
  rimraf.sync(db2Path)
  t.pass('delete db2 folder to start clean')

  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()
  startMeasure(t, 'migrate (+db2)')
  sbot.db2migrate.start()

  pull(
    sbot.db2migrate.progress(),
    pull.filter((progress) => progress === 1),
    pull.take(1),
    pull.drain(async () => {
      endMeasure(t, 'migrate (+db2)')
      await new Promise((resolve) => sbot.db.onDrain(resolve))
      sbot.close(true, () => ended.resolve())
    })
  )

  await ended.promise
})

test('migrate continuation (+db2)', async (t) => {
  rimraf.sync(db2Path)
  t.pass('delete db2 folder to start clean')

  let sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()
  sbot.db2migrate.start()

  pull(
    sbot.db2migrate.progress(),
    pull.filter((progress) => progress > 0.9),
    pull.take(1),
    pull.drain(async () => {
      sbot.db2migrate.stop()
      await new Promise((resolve) => sbot.db.onDrain(resolve))
      await new Promise((resolve) => sbot.close(true, resolve))
      await sleep(500) // some silence
      t.pass('migrated 90%, will reset sbot')

      sbot = SecretStack({ appKey: caps.shs })
        .use(require('../'))
        .call(null, { keys, path: dir })

      global.gc()
      await sleep(500)
      updateMaxRAM() // will report later, just to make the report order pretty

      startMeasure(t, 'migrate continuation (+db2)')
      sbot.db2migrate.start()

      pull(
        sbot.db2migrate.progress(),
        pull.filter((progress) => progress === 1),
        pull.take(1),
        pull.drain(async () => {
          endMeasure(t, 'migrate continuation (+db2)')
          await new Promise((resolve) => sbot.db.onDrain(resolve))
          sbot.close(true, () => ended.resolve())
        })
      )
    })
  )

  await ended.promise
})

test('Memory usage without indexes', (t) => {
  t.pass(`memory usage without indexes: ${reportMem()}`)
  fs.appendFileSync(
    reportPath,
    `| Memory usage without indexes | ${reportMem()} |\n`
  )
  t.end()
})

test('initial indexing', async (t) => {
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()

  startMeasure(t, 'initial indexing')
  sbot.db.query(
    where(type('post')),
    descending(),
    paginate(1),
    toCallback((err, { results, total }) => {
      endMeasure(t, 'initial indexing')

      t.error(err)
      if (total === 0) t.fail('should respond with msgs')
      if (results.length !== 1) t.fail('should respond with 1 msg')
      if (!results[0].value.content.text.includes('LATESTMSG'))
        t.fail('should have LATESTMSG')
      updateMaxRAM()
      global.gc()
      sbot.close(true, () => ended.resolve())
    })
  )

  await ended.promise
})

test('initial indexing maxcpu 86', async (t) => {
  rimraf.sync(indexesPath)
  t.pass('delete indexes folder to start clean')

  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, { keys, path: dir, db2: { maxCpu: 86 } })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()

  startMeasure(t, 'initial indexing maxcpu=86')
  sbot.db.query(
    where(type('post')),
    descending(),
    paginate(1),
    toCallback((err, { results, total }) => {
      endMeasure(t, 'initial indexing maxcpu=86')

      t.error(err)
      if (total === 0) t.fail('should respond with msgs')
      if (results.length !== 1) t.fail('should respond with 1 msg')
      if (!results[0].value.content.text.includes('LATESTMSG'))
        t.fail('should have LATESTMSG')
      updateMaxRAM()
      global.gc()
      sbot.close(true, () => ended.resolve())
    })
  )

  await ended.promise
})

test('initial indexing compat', async (t) => {
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .use(require('../compat'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()

  startMeasure(t, 'initial indexing compat')
  sbot.db.onDrain('base', () => {
    sbot.db.onDrain('ebt', () => {
      endMeasure(t, 'initial indexing compat')

      updateMaxRAM()
      global.gc()
      sbot.close(true, () => ended.resolve())
    })
  })

  await ended.promise
})

test('Two indexes updating concurrently', async (t) => {
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .use(require('../compat'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()
  const done = multicb({ pluck: 1 })

  startMeasure(t, 'two indexes updating concurrently')
  sbot.db.query(where(type('about')), toCallback(done()))
  sbot.db.query(where(and(type('about'), isPublic())), toCallback(done()))

  done((err) => {
    endMeasure(t, 'two indexes updating concurrently')
    if (err) t.fail(err)
    updateMaxRAM()
    global.gc()
    sbot.close(true, () => ended.resolve())
  })

  await ended.promise
})

test.skip('ssb-threads and ssb-friends', async (t) => {
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .use(require('ssb-friends'))
    .use(require('ssb-threads'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()

  startMeasure(t, 'ssb-threads and ssb-friends')
  pull(
    sbot.threads.publicSummary({ allowlist: ['post', 'contact'] }),
    pull.take(1),
    pull.collect(async (err, threads) => {
      endMeasure(t, 'ssb-threads and ssb-friends')
      if (err) t.fail(err)
      if (threads.length !== 1) t.fail('missing results')
      updateMaxRAM()
      global.gc()
      await sleep(2000) // wait for jitdb indexes to save to disk
      sbot.close(true, () => ended.resolve())
    })
  )

  await ended.promise
})

test.skip('ssb-threads and ssb-friends again', async (t) => {
  const sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .use(require('ssb-friends'))
    .use(require('ssb-threads'))
    .call(null, { keys, path: dir })

  await sleep(500) // some silence to make it easier to read the CPU profiler

  const ended = DeferredPromise()

  startMeasure(t, 'ssb-threads and ssb-friends again')
  pull(
    sbot.threads.publicSummary({ allowlist: ['post', 'contact'] }),
    pull.take(1),
    pull.collect(async (err, threads) => {
      endMeasure(t, 'ssb-threads and ssb-friends again')
      if (err) t.fail(err)
      if (threads.length !== 1) t.fail('missing results')
      updateMaxRAM()
      global.gc()
      await sleep(2000) // wait for jitdb indexes to save to disk
      sbot.close(true, () => ended.resolve())
    })
  )

  await ended.promise
})

const KEY1 = '%Xwdpu9gRe8wl4i0ssKyU24oGYKXW75hjE5VCbEB9bmM=.sha25' // root post
const KEY2 = '%EpzOw6sOBb4RGtofVD43GnfImoiw6NzEEsraHsNXF1g=.sha25' // contact
const KEY3 = '%55wBq68+p45q7/OuPgL+TC07Ifx8ihEW93u/EZaYv6c=.sha256' // another post
const AUTHOR1 = '@ZngOKXHjrvG+cy7Gjx5pSFunUqcePfmDQQxoUlHFUdU=.ed2551'
const AUTHOR2 = '@58u/J9+5bOXeYRDCYQ9cJ7kklghIpQFPBYxlhKq1/qs=.ed2551'
const REBOOT = 'reboot'

const queries = {
  'key one initial': [where(key(KEY1))],

  'key two': [where(key(KEY2))],

  'key one again': [where(key(KEY1))],

  [REBOOT]: true,

  'reboot and key one again': [where(key(KEY1))],

  'latest root posts': [
    where(and(type('post'), isRoot(), isPublic())),
    startFrom(0),
    paginate(25),
    descending(),
  ],

  'latest posts': [
    where(and(type('post'), isPublic())),
    startFrom(0),
    paginate(25),
    descending(),
  ],

  'votes one initial': [where(votesFor(KEY1))],

  'votes again': [where(votesFor(KEY3))],

  hasRoot: [where(hasRoot(KEY1))],

  'hasRoot again': [where(hasRoot(KEY3))],

  'author one posts': [
    where(and(type('post'), author(AUTHOR1), isPublic())),
    startFrom(0),
    paginate(25),
    descending(),
  ],

  'author two posts': [
    where(and(type('post'), author(AUTHOR2), isPublic())),
    startFrom(0),
    paginate(25),
    descending(),
  ],

  'dedicated author one posts': [
    where(and(type('post'), author(AUTHOR1, { dedicated: true }), isPublic())),
    startFrom(0),
    paginate(25),
    descending(),
  ],

  'dedicated author one posts again': [
    where(and(type('post'), author(AUTHOR1, { dedicated: true }), isPublic())),
    startFrom(0),
    paginate(25),
    descending(),
  ],
}

let sbot
test('setup', (t) => {
  sbot = SecretStack({ appKey: caps.shs })
    .use(require('../'))
    .call(null, { keys, path: dir })
  t.end()
})

for (const title in queries) {
  test(title, (t) => {
    if (title === REBOOT) {
      sbot.close(true, () => {
        sbot = SecretStack({ appKey: caps.shs })
          .use(require('../'))
          .call(null, { keys, path: dir })
        t.end()
      })
      return
    }

    startMeasure(t, title)
    sbot.db.query(
      ...queries[title],
      toCallback((err) => {
        endMeasure(t, title)
        if (err) t.fail(err)
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

test('Indexes folder size', (t) => {
  trammel(indexesPath).then((size) => {
    t.pass(`indexes folder size: ${size}`)
    fs.appendFileSync(reportPath, `| Indexes folder size | ${size} |\n`)
    t.end()
  })
})

test('teardown', (t) => {
  sbot.close(true, t.end)
})
