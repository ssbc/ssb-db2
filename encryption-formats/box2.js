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
  const ownDMKeysCache = []
  const sharedDMKeysCache = new Map()
  const groupKeysCache = new Map()

  function addOwnDMKey(key) {
    ownDMKeysCache.push(key)
  }

  function ownDMKeys() {
    return ownDMKeysCache.map((key) => {
      return { key, scheme: keySchemes.feed_id_self }
    })
  }

  const buildSharedDMKey = directMessageKey.easy(config.keys)

  function sharedDMKey(author) {
    if (!sharedDMKeysCache.has(author)) {
      sharedDMKeysCache.set(author, buildSharedDMKey(author))
    }
    return sharedDMKeysCache.get(author)
  }

  function addGroupKey(id, key) {
    groupKeysCache.set(id, key)
  }

  function groupKey(id) {
    if (groupKeysCache.has(id)) {
      return { key: groupKeysCache.get(id), scheme: keySchemes.private_group }
    } else {
      return undefined
    }
  }

  function groupKeys() {
    return [...groupKeysCache.values()].map((key) => {
      return { key, scheme: keySchemes.private_group }
    })
  }

  return {
    addOwnDMKey,
    ownDMKeys,

    sharedDMKey,

    addGroupKey,
    groupKey,
    groupKeys,
  }
}

module.exports = {
  name: 'box2',
  init: function init(ssb, config) {
    const keysManager = makeKeysManager(config)

    function isGroup(recp) {
      return keysManager.groupKey(recp) !== undefined
    }

    function isFeed(recp) {
      return (
        Ref.isFeed(recp) || isFeedSSBURI(recp) || isBendyButtV1FeedSSBURI(recp)
      )
    }

    const encryptionFormat = {
      name: 'box2',
      suffix: 'box2',

      onReady(cb) {
        // FIXME: load ssb-keyring here
        cb()
      },

      getRecipients(opts) {
        const recps = opts.recps || opts.content.recps
        const selfId = opts.keys.id
        if (!recps) return null
        if (recps.length === 0) return null

        const validRecps = recps
          .filter((recp) => typeof recp === 'string')
          .filter((recp) => recp === selfId || isGroup(recp) || isFeed(recp))

        if (validRecps.length === 0) {
          // prettier-ignore
          throw new Error(`no box2 keys found for recipients: ${recps}`)
        }
        if (validRecps.length > 16) {
          // prettier-ignore
          throw new Error(`private-group spec allows maximum 16 slots, but you've tried to use ${validRecps.length}`)
        }
        // FIXME: move these validations to ssb-groups
        // if (validRecps.filter(isGroup).length === 0) {
        //   // prettier-ignore
        //   throw new Error(`no group keys found among recipients: ${recps}`)
        // }
        // if (!isGroup(validRecps[0])) {
        //   // prettier-ignore
        //   throw new Error(`first recipient must be a group, but you've tried to use ${validRecps[0]}`)
        // }
        if (validRecps.filter(isGroup).length > 1) {
          // prettier-ignore
          throw new Error(`private-group spec only supports one group recipient, but you've tried to use ${validRecps.filter(isGroup).length}`)
        }

        return validRecps.reduce((acc, recp) => {
          if (recp === selfId) return [...acc, ...keysManager.ownDMKeys()]
          else if (isGroup(recp)) return [...acc, keysManager.groupKey(recp)]
          else if (isFeed(recp)) return [...acc, keysManager.sharedDMKey(recp)]
        }, [])
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

    if (ssb.db) ssb.db.installEncryptionFormat(encryptionFormat)

    return {
      addOwnDMKey: keysManager.addOwnDMKey,
      addGroupKey: keysManager.addGroupKey,
    }
  },
}
