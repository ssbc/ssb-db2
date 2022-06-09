// SPDX-FileCopyrightText: 2022 Andre 'Staltz' Medeiros
//
// SPDX-License-Identifier: LGPL-3.0-only

const BFE = require('ssb-bfe')
const Ref = require('ssb-ref')
const { isFeedSSBURI, isBendyButtV1FeedSSBURI } = require('ssb-uri2')
const { keySchemes } = require('private-group-spec')
const { box, unboxKey, unboxBody } = require('envelope-js')
const { directMessageKey, SecretKey } = require('ssb-private-group-keys')

function makeKeysManager(config) {
  const dmCache = {}

  const buildDMKey = directMessageKey.easy(config.keys)

  function sharedDMKey(author) {
    if (!dmCache[author]) dmCache[author] = buildDMKey(author)

    return dmCache[author]
  }

  const ownKeys = []

  function addDMKey(key) {
    ownKeys.push(key)
  }

  function ownDMKeys() {
    return ownKeys.map((key) => {
      return { key, scheme: keySchemes.feed_id_self }
    })
  }

  const allGroupKeys = {}

  function addGroupKey(id, key) {
    allGroupKeys[id] = key
  }

  function groupKey(id) {
    const key = allGroupKeys[id]
    if (key) return { key, scheme: keySchemes.private_group }
    else return undefined
  }

  function groupKeys() {
    return Object.values(allGroupKeys).map((key) => {
      return { key, scheme: keySchemes.private_group }
    })
  }

  return {
    ownDMKeys,
    TFKId: BFE.encode(config.keys.id),
    sharedDMKey,
    addDMKey,

    addGroupKey,
    groupKey,
    groupKeys,
  }
}

module.exports = {
  name: 'box2',
  init: function init(ssb, config) {
    const keysManager = makeKeysManager(config)

    function isGroup(recipient) {
      return false // FIXME: uh what
    }

    function isFeed(x) {
      // FIXME: uh what
      return Ref.isFeed(x) || isFeedSSBURI(x) || isBendyButtV1FeedSSBURI(x)
    }

    const encryptionFormat = {
      name: 'box2',
      suffix: 'box2',

      onReady(cb) {
        // FIXME:
      },

      getRecipients(opts) {
        if (!opts.recps && !opts.content.recps) return null
        const recipients = (opts.recps || opts.content.recps).reduce(
          (acc, recp) => {
            if (recp === opts.keys.id)
              return [...acc, ...keysManager.ownDMKeys()]
            else if (isGroup(recp)) return [...acc, keysManager.groupKey(recp)]
            else return [...acc, keysManager.sharedDMKey(recp)]
          },
          []
        )

        if (recipients.length === 0) {
          // prettier-ignore
          throw new Error(`no keys found for recipients: ${opts.recps || opts.content.recps}`)
        }
        if (recipients.length > 16) {
          // prettier-ignore
          throw new Error(`private-group spec allows maximum 16 slots, but you've tried to use ${recipients.length}`)
        }

        // groupId can only be in first "slot"
        // FIXME: "setIsGroup" etc
        // if (!isGroup(recipients[0]) && !isFeed(recipients[0]))
        //   throw new Error('first recipient must be a group or feed')

        // if (recipients.length > 1 && !recipients.slice(1).every(isFeed)) {
        //   throw new Error('only feed IDs are supported as secondary recipients')
        // }

        return recipients
      },

      encrypt(plaintextBuf, recipients, opts) {
        const msgSymmKey = new SecretKey().toBuffer()
        const authorIdBFE = BFE.encode(opts.keys.id)
        const previousMsgIdBFE = BFE.encode(
          opts.previous ? opts.previous.key : null
        )

        const ciphertextBuf = box(
          plaintextBuf,
          authorIdBFE,
          previousMsgIdBFE,
          msgSymmKey,
          recipients
        )

        return ciphertextBuf
      },

      decrypt(ciphertextBuf, opts) {
        const authorBFE = BFE.encode(opts.author)
        const previousBFE = BFE.encode(opts.previous)

        const trialGroupKeys = keysManager.groupKeys()
        const readKeyFromGroup = unboxKey(
          ciphertextBuf,
          authorBFE,
          previousBFE,
          trialGroupKeys,
          { maxAttempts: 1 }
        )
        // NOTE the group recp is only allowed in the first slot,
        // so we only test group keys in that slot (maxAttempts: 1)
        if (readKeyFromGroup)
          return unboxBody(
            ciphertextBuf,
            authorBFE,
            previousBFE,
            readKeyFromGroup
          )

        const trialDMKeys =
          opts.author !== ssb.id
            ? [keysManager.sharedDMKey(opts.author), ...keysManager.ownDMKeys()]
            : keysManager.ownDMKeys()

        const readKey = unboxKey(
          ciphertextBuf,
          authorBFE,
          previousBFE,
          trialDMKeys,
          {
            maxAttempts: 16,
          }
        )

        if (readKey)
          return unboxBody(ciphertextBuf, authorBFE, previousBFE, readKey)
        else return null
      },
    }

    ssb.db.addEncryptionFormat(encryptionFormat)

    // FIXME: is this the nicest approach???
    return {
      addOwnDMKey: keysManager.addDMKey,
    }
  },
}
