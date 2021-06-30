const directMessageKey = require('ssb-tribes/lib/direct-message-key')
const SecretKey = require('ssb-tribes/lib/secret-key')
const { FeedId } = require('ssb-tribes/lib/cipherlinks')
const { keySchemes } = require('private-group-spec')

module.exports = function (config) {
  const dmCache = {}

  const buildDMKey = directMessageKey.easy(config.keys)

  function sharedDMKey(author) {
    if (!dmCache[author]) dmCache[author] = buildDMKey(author)

    return {
      key: dmCache[author],
      scheme: keySchemes.feed_id_dm,
    }
  }

  // FIXME: fetch seed and derive?
  const FIXMEKEY = Buffer.from(
    '30720d8f9cbf37f6d7062826f6decac93e308060a8aaaa77e6a4747f40ee1a76',
    'hex'
  )
  const ownKey = {
    key: FIXMEKEY, //new SecretKey().toBuffer(),
    scheme: keySchemes.feed_id_self,
  }

  // FIXME: maybe if a feed has a meta feed, then we can assume it
  // does box2 as well
  function supportsBox2(feedId) {
    if (config.db2 && config.db2.alwaysbox2) return true
    else return feedId.endsWith('.bbfeed-v1') || feedId.endsWith('.fusion-v1')
  }

  return {
    ownKey,
    TFKId: new FeedId(config.keys.id).toTFK(),
    sharedDMKey,
    supportsBox2,
  }
}
