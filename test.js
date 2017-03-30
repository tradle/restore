
const Promise = require('bluebird')
const co = Promise.coroutine
const test = require('tape')
const contexts = require('@tradle/engine/test/contexts')
const helpers = require('@tradle/engine/test/helpers')
const { utils, constants } = require('@tradle/engine')
const { TYPE, SEQ } = constants
const makeFriends = Promise.promisify(contexts.nFriends)
const { conversation, batchifyMonitor } = require('./')
const { request, respond, monitorMissing } = conversation
const noop = function () {}

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
    req: req.object,
    sent: true
  })

  const receivedSeqs = msgs.map(msg => msg[SEQ])
  t.same(receivedSeqs, seqs)

  try {
    yield respond({
      node: bob,
      req: req.object,
      sent: true
    })

    t.fail('validation should have failed')
  } catch (err) {
    t.ok(/restricted|unauthorized/i.test(err.message))
  }

  t.end()
}))

test('monitor', co(function* (t) {
  const friends = yield makeFriends(2)
  const [alice, bob] = friends.map(utils.promisifyNode)

  let i = 0
  let togo = 10
  let n = 10
  alice._send = function (msg, recipientInfo, cb) {
    if (i++ % 2 === 0) return cb() // drop message

    bob.receive(msg, alice._recipientOpts, cb)
  }

  const bobReceived = {}
  bob.on('message', function (msg, from) {
    const seq = msg.object[SEQ]
    t.notOk(seq in bobReceived)
    bobReceived[seq] = true
    if (--togo === 0) t.end()
  })

  const monitor = monitorMissing({
    node: bob,
    counterparty: alice.permalink
  })

  batchifyMonitor({ monitor, debounce: 100 }).on('batch', co(function* (seqs) {
    const req = yield request({
      node: bob,
      from: alice.permalink,
      seqs
    })

    const res = yield respond({
      node: alice,
      req: req.object,
      sent: true
    })

    res.forEach(msg => {
      bob.receive(msg, alice._recipientOpts, noop)
    })
  }))

  const sends = new Array(n).fill(0).map((n, i) => {
    return alice.signAndSend({
      to: bob._recipientOpts,
      object: {
        [TYPE]: 'something',
        count: i
      }
    })
  })

  yield Promise.all(sends)
}))
