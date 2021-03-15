const pull = require('pull-stream')
const cat = require('pull-cat')
const { live, toPullStream } = require('../operators')

// exports.name is blank to merge into global namespace

exports.manifest = {
  createLogStream: 'source',
}

exports.init = function (sbot) {
  sbot.createLogStream = function createLogStream(opts) {
    // Apply default values
    opts = opts || {}
    const optsKeys = opts.keys === false ? false : true
    const optsValues = opts.values === false ? false : true
    const optsSync = opts.sync === false ? false : true
    const optsLive = opts.live === true ? true : false
    const optsOld = opts.old === true ? true : false

    function format(msg) {
      if (!optsKeys && optsValues) return msg.value
      else if (optsKeys && !optsValues) return msg.key
      else return msg
    }

    const old$ = pull(sbot.db.query(toPullStream()), pull.map(format))
    const sync$ = pull.values([{ sync: true }])
    const live$ = pull(sbot.db.query(live(), toPullStream()), pull.map(format))

    if (!optsLive) return old$
    if (optsOld && optsSync) return cat([old$, sync$, live$])
    if (optsOld && !optsSync) return cat([old$, live$])
    if (!optsOld && optsSync) return cat([sync$, live$])
    if (!optsOld && !optsSync) return live$
  }
}
