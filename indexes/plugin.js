const Obv = require('obv')
const Level = require('level')
const path = require('path')

module.exports = function (log, dir, name, version, debug,
                           handleData, writeData) {
  var seq = Obv()
  seq.set(-1)

  const indexesPath = path.join(dir, "indexes", name)

  if (typeof window === 'undefined') { // outside browser
    const mkdirp = require('mkdirp')
    mkdirp.sync(indexesPath)
  }

  var level = Level(indexesPath)
  const META = '\x00'
  
  const chunkSize = 512
  var isLive = false
  var processed = 0
  
  function updateIndexes() {
    const start = Date.now()

    let unWritten = 0

    function writeBatch() {
      level.put(META, { version, seq: seq.value },
                { valueEncoding: 'json' },
                (err) => { if (err) throw err })

      writeData()
    }
    
    function onData(data) {
      unWritten = handleData(data, isLive)
      seq.set(data.seq)
      processed++
      
      if (unWritten > chunkSize || isLive)
        writeBatch()
    }
    
    log.stream({ gt: seq.value }).pipe({
      paused: false,
      write: onData,
      end: () => {
        if (unWritten > 0)
          writeBatch()
        
        debug(`${name} index scan time: ${Date.now()-start}ms, items: ${processed}`)

        isLive = true
        log.stream({ gt: seq.value, live: true }).pipe({
          paused: false,
          write: onData
        })
      }
    })
  }

  level.get(META, { valueEncoding: 'json' }, (err, data) => {
    debug(`got ${name} index status:`, data)

    if (data && data.version == version) {
      seq.set(data.seq)
      updateIndexes()
    } else
      level.clear(updateIndexes)
  })

  return { level, seq }
}
