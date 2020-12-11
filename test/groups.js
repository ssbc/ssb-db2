const test = require('tape')
const ssbKeys = require('ssb-keys')
const path = require('path')
const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const DB = require('../db')

const dir = '/tmp/ssb-db2-group'

rimraf.sync(dir)
mkdirp.sync(dir)

const keys = {
  curve: 'ed25519',
  public: 'hw+2CI4p9kOt4orwcl/4HrTLTZLXQ8r6RUCB78bX1pQ=.ed25519',
  private:
    '1qcrLnGKxVw1MJHn6yHezD5EMD/SfitID/A2FQc+u+GHD7YIjin2Q63iivByX/getMtNktdDyvpFQIHvxtfWlA==.ed25519',
  id: '@hw+2CI4p9kOt4orwcl/4HrTLTZLXQ8r6RUCB78bX1pQ=.ed25519',
}

const db = DB.init({}, dir, {
  path: dir,
  keys,
})

test('Base', (t) => {
  const groupInitMsg = {
    previous: null,
    sequence: 1,
    author: '@9Y6gUScVUtxm5qujlDQQhCZZ3+DQBIZh3vtfFvCdVWg=.ed25519',
    timestamp: 1438787025000,
    hash: 'sha256',
    content:
      'oIogDumL0H7+2TzipPTqZXmwx+04i9aE2mCDOb+hE0Pe+b0pGW0BUdVafzHdiGuDq7/r6Bi8wcNXhYoB4bSMlhNrdK7FJ40VoqXITcEHFwiQTxrkFxhD35oh2+J2J73jxxSXRzvn1fFgu+E7t22WfMkyfh3VpZSYniuh297KzwQBPDA5pjBMskp4pnuMk0ZYcxaGUrP33Q==.box2',
    signature:
      'WhIDa6MCpyK5DYiNKgZFLXrH4UjT5EDAuYL/ChK0Qcff55m95pMSivoKowpWgkPgiix6XLqoJrHmJkHnT8qCCg==.sig.ed25519',
  }
  const groupMsg = {
    previous: '%NH6Pm6XYwFDYic7jMUzzm8YrNAUPbfy0P7Q4/mmJb/Y=.sha256',
    sequence: 2,
    author: '@9Y6gUScVUtxm5qujlDQQhCZZ3+DQBIZh3vtfFvCdVWg=.ed25519',
    timestamp: 1438787145000,
    hash: 'sha256',
    content:
      '6OOeZoZ+i2ThGT8Ldt8yqbS0S3tB41C7qvPdQZ8HAp5w7+YgSelnKFcB6kQWu45WqWl/oyoc/Kn9141gsW7DwtPp0PRbzhgja21ucfHuDfy9oWu/WVWIv7mbwQ7jSujJxPy86eYYxhLvkwd6L4fQiUADa3iTFQ9nzYaw79ZQ/9N9Jwcl0fHepy8+mtipfFArQfrxor4xboa+4AHr5mdJJZHBS8I/7dcWtGcn636FmSamMp+skEblyN/NQOz7B4AdJzOGhyeo0YGlKrWUjK2EIoFC20YFRqaSHz+KhbrA6LJ/GASgeK9bBBWPYyXhKLqvep8H7eDYmnf5pouuegbk3ZEx+3AQAt1M4F4R5ow5L/zmyg8sRX/3isRk1tRPNgPhRk0ee8x35m+GRC/QVG/meY4QQczrKa0qmn5XtDvAp8sC5pEdyiyxsBaUAy4lF3bUCpZBNg==.box2',
    signature:
      'RdJZJ4p9I9aLnluqDSBlWl91VOa6Atsvq6uNFLDayz0bJzs2WC8Md50WiEYkiXdbl8akOv5Dn4kCZZ1sFG74DA==.sig.ed25519',
  }
  const groupInviteMsg = {
    previous: '%smnqZPO9Jzat+TGx5hc90w1/pUh3TgG6JesBfe3BvCk=.sha256',
    sequence: 3,
    author: '@9Y6gUScVUtxm5qujlDQQhCZZ3+DQBIZh3vtfFvCdVWg=.ed25519',
    timestamp: 1438787265000,
    hash: 'sha256',
    content:
      'ykHZmJEjoFbaLjzM+bStnR0bT+oDsxE9YgFboFuiuE2XdvCmF9EFFyDeiaVv6H/S6S645pdkvR5DvvPf6SFIaYDfiGoShCiQkOOu2ekWSztsDeHi/zwdF01brufKfIgSVfcVV9BOpS2IxnJaalx6LOJQ9umqVUqYIXGvK907ZaluQhM8NReiVCs/F8fwwhEq4h77V9eagHVW5NY05MsyPzjxR60opgkhMVrO1x6L+E5HDBBp5cx19yCDs+LVaasPI8PcBGf8KfjQMHJrzcFeyQzFccbAzHvKgt1AgsixUmznxUmuOv9tnMaKq0btwagqY+EfZcL3pj5RZb3djZzXCSzp/ZjAHJeSdv/J5Edwc3+iHPSjZNOIJgc/wBm5G28UXDGNxSKxcYUYVinAxA6EfFhGLKv96kZOC6v1gDSSgq/j1pRKt380mYcRLm2UKuG1+bpAOm8jKCu5PMVvTXgM2dDjzFYFgWwlyqw7ZYy4GtaQILORJUBhbC+HdQricx3J4nyz0Q+LR9nVgE/EoUE9zQlmSH4bkFTOZCNumWKHisXUfAELSbeCkX4Wd0rCFM7DD8AfoDewHHvo07kXYXGAoO/jTJChNgSVixYaugk/+byGP8uxurxJ/h7X+YmRBHLuw5PdO4m+TL2udNxU9Xnm3SfVrTz++BkQFloaTU1fh2aLP+iga06G8l7sWhJ0C6UOY+OHD+DECzhDvxEFKESTz9NviznsjRM6mKEu7t+MLloJX8cHM5OkfmvBL6QyAc2rcmKp5Amsj3hv9CbBny0wDADTXTK/SUmI/1VTFuVxDGw87/1E/INb1aEwq10npR2PbG/RMjgapOGsFYSi6MPYhQEkptyexy9RuQlFqDBJMoZehFZHpCeiwo/+zS9pJ8WT+vrnyLs4Axsdpt4D9w4ORDHH.box2',
    signature:
      'StvrZ8jmT8YkwkPzPzj4iKDowRN+UkJnA2HvSqxq0GXZ4AJpTeoG3Brbu4UfAPPVcLmxkngimYpTQM8wwAMMAg==.sig.ed25519',
  }
  const groupKey = '4uEN6ltQBgSbZlJm+FCAAd0wnEGoJzFc6soeikAVt6g='

  db.add(groupInitMsg, () => {
    db.add(groupMsg, () => {
      db.add(groupInviteMsg, (err, imported) => {
        db.onDrain('base', () => {
          db.get(imported.key, (err, msg) => {
            t.equal(msg.content.groupKey, groupKey, 'extracted key')
            t.end()
          })
        })
      })
    })
  })
})
