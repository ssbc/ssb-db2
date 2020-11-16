/*
 format:
 id => {
   full,
   syncedProfile,
   syncedContacts,
   syncedMessages,
 }
*/

module.exports = function (dir) {
  const AtomicFile = require('atomic-file')
  const debounce = require('lodash.debounce')
  const path = require('path')

  const queue = require('../waiting-queue')()
  var state = {}

  var f = AtomicFile(path.join(dir, 'db2', 'indexes', 'partial.json'))

  f.get((err, data) => {
    if (data)
      state = data.state

    queue.done(null, state)
  })

  function atomicSave()
  {
    f.set({ state }, (err) => {
      if (err) console.error("error saving partial", err)
    })
  }
  var saveState = debounce(atomicSave, 1000, { leading: true })

  function save(cb) {
    saveState()
    cb()
  }

  return {
    updateState: function(feedId, updateFeedState, cb) {
      queue.get(() => {
        let feedState = state[feedId] || {}
        state[feedId] = Object.assign(feedState, updateFeedState)
        save(cb)
      })
    },

    removeFeed: function(feedId, cb) {
      queue.get(() => {
        delete state[feedId]
        save(cb)
      })
    },

    get: queue.get,
    getSync: function() {
      return state
    },

    remove: function(cb) {
      f.destroy(cb)
    }
  }
}
