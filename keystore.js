const directMessageKey = require('ssb-tribes/lib/direct-message-key')
const SecretKey = require('ssb-tribes/lib/secret-key')
const { FeedId } = require('ssb-tribes/lib/cipherlinks')

module.exports = function (keys) {
  const dmCache = {}

  const buildDMKey = directMessageKey.easy(keys)

  function sharedDMKey(author) {
    if (!dmCache[author]) dmCache[author] = buildDMKey(authorId)

    return dmCache[author]
  }

  // FIXME: fetch seed and derive?
  const ownKey = new SecretKey().toBuffer()

  // FIXME: maybe if a feed has a meta feed, then we can assume it
  // does box2 as well
  function supportsBox2(feedId) {
    return feedId.endsWith('.bbfeed-v1') || feedId.endsWith('.fusion-v1')
  }

  return {
    ownKey,
    TFKId: new FeedId(keys.id).toTFK(),
    sharedDMKey,
    supportsBox2,
  }
}
