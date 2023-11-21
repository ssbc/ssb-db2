// SPDX-FileCopyrightText: 2021 Anders Rune Jensen
//
// SPDX-License-Identifier: LGPL-3.0-only

const pull = require('pull-stream')
const cat = require('pull-cat')
const Hookable = require('hoox')
const { descending, live, toPullStream } = require('../operators')

// exports.name is blank to merge into global namespace

exports.manifest = {
  createLogStream: 'source',
}

exports.init = function (sbot) {
  sbot.createLogStream = Hookable(function createLogStream(opts) {
    // Apply default values
    opts = opts || {}
    const optsKeys = opts.keys === false ? false : true
    const optsValues = opts.values === false ? false : true
    const optsSync = opts.sync === false ? false : true
    const optsLive = opts.live === true ? true : false
    const optsOld = opts.old === true ? true : false
    const optsLimit = typeof opts.limit === 'number' ? opts.limit : -1
    const optsReverse = opts.reverse === true ? true : false

    function format(msg) {
      if (!optsKeys && optsValues) return msg.value
      else if (optsKeys && !optsValues) return msg.key
      else return msg
    }

    function applyLimit(source) {
      if (optsLimit < 0) return source
      else if (optsLimit === 0) return pull.empty()
      else return pull(source, pull.take(optsLimit))
    }

    const old$ = pull(
      sbot.db.query(optsReverse ? descending() : null, toPullStream()),
      pull.map(format)
    )
    const sync$ = pull.values([{ sync: true }])
    const live$ = pull(sbot.db.query(live(), toPullStream()), pull.map(format))

    if (!optsLive) return applyLimit(old$)
    if (optsOld && optsSync) return applyLimit(cat([old$, sync$, live$]))
    if (optsOld && !optsSync) return applyLimit(cat([old$, live$]))
    if (!optsOld && optsSync) return applyLimit(cat([sync$, live$]))
    if (!optsOld && !optsSync) return applyLimit(live$)
  })
}
