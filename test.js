
const Promise = require('bluebird')
const co = Promise.coroutine
const test = require('tape')
const contexts = require('@tradle/engine/test/contexts')
const helpers = require('@tradle/engine/test/helpers')
const { utils, constants } = require('@tradle/engine')
const { TYPE, SEQ } = constants
const makeFriends = Promise.promisify(contexts.nFriends)
const { conversation } = require('./')
const { request, respond } = conversation

test('basic request/respond', co(function* (t) {
  const friends = yield makeFriends(3)
  helpers.connect(friends)

  const [alice, bob, carol] = friends.map(utils.promisifyNode)
  const sends = new Array(4).fill(0).map((n, i) => {
    return alice.signAndSend({
      to: bob._recipientOpts,
      object: {
        [TYPE]: 'something',
        count: i
      }
    })
  })

  yield Promise.all(sends)

  const seqs = [0, 2]
  const req = yield request({
    node: bob,
    from: alice.permalink,
    seqs
  })

  const msgs = yield respond({
    node: alice,
    req: req.object
  })

  const receivedSeqs = msgs.map(msg => msg[SEQ])
  t.same(receivedSeqs, seqs)

  try {
    yield respond({
      node: bob,
      to: carol.permalink,
      req: req.object
    })

    t.fail('validation should have failed')
  } catch (err) {
    t.ok(/unauthorized/i.test(err.message))
  }

  t.end()
}))
