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

  // fetch seed and derive?
  const ownKey = new SecretKey().toBuffer()

  return {
    ownKey,
    TFKId: new FeedId(keys.id).toTFK(),
    sharedDMKey,
  }
}
