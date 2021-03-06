const { EventEmitter } = require('events')
const Promise = require('any-promise')
const co = require('co').wrap
const promisify = require('pify')
const collect = promisify(require('stream-collector'))
const through = require('through2')
const pump = require('pump')
const omit = require('object.omit')
const debug = require('debug')('tradle:restore')
const once = require('once')
const extend = require('xtend/mutable')
const { typeforce, utils, constants } = require('@tradle/engine')
const Status = require('@tradle/engine/lib/status')
const { TYPE, INDEX_SEPARATOR } = constants
const RESTORE_REQUEST = 'tradle.RestoreRequest'
const addAuthor = promisify(utils.addAuthor)
const conversation = {}

conversation.respond = co(function* ({ node, req, sent, received }) {
  // TODO: support ranges, optimize
  if (!xor(sent, received)) {
    throw new Error('expected "sent" OR "received"')
  }

  const validate = promisify(node.validator.validate)
  // only restore for original conversation participant
  const { from, to } = req
  const me = node.permalink
  if (sent && me !== from) {
    throw new Error('restricted to restoring messages I sent')
  }

  if (received && me !== to) {
    throw new Error('restricted to restoring messages I received')
  }

  const them = me === from ? to : from
  const wrapper = { object: req }
  utils.addLinks(wrapper)
  yield addAuthor(node, wrapper)

  if (wrapper.author.permalink !== them) {
    throw new Error('unauthorized')
  }

  yield validate(wrapper)

  const { tip, seqs } = req
  const msgs = pump(
    streamSent({ node, from, to, body: false }),
    through.obj(function (data, enc, cb) {
      const { value } = data
      const { seq, link } = data
      const beforeTip = typeof tip === 'undefined' || seq <= tip
      if (beforeTip && seqs.indexOf(seq) === -1) return cb()

      node.keeper.get(link, function (err, body) {
        if (err) {
          debug('missing message: ' + link)
          return cb()
        }

        cb(null, body)
      })
    })
  )

  return collect(msgs)
})

/**
 * Request a set of messages
 * @param {@tradle/engine}        options.node
 * @param {String}                options.counterparty permalink of counterparty
 * @param {Boolean}               options.outbound true if you're requesting messages you sent
 * @param {Array[Number]}         options.seqs seq numbers of messages you need
 * @param {Number}                options.tip  the seq of the latest message you have
 * @return {Promise}              Promise that resolves to signed request object
 */
conversation.request = co(function* (opts) {
  const { node, counterparty, outbound, seqs=[], tip } = opts
  const sign = promisify(node.sign.bind(node))
  const object = getFromTo(opts)
  object[TYPE] = RESTORE_REQUEST
  object.seqs = seqs
  if (typeof tip !== 'undefined') object.tip = tip

  return sign({ object })
})

conversation.monitorMissing = function monitorMissing (opts) {
  typeforce({
    node: 'Object',
    counterparty: 'String',
    outbound: '?Boolean'
  }, opts)

  let { node, counterparty, checkpoint=0, outbound } = opts
  const { objects } = node
  let stream
  let tip

  const ee = new EventEmitter()
  conversation.monitorTip({
    node,
    counterparty,
    outbound,
    onChange: onNewTip
  })

  ee.reset = function reset (start=checkpoint) {
    checkpoint = start
    ee.stop()

    stream = node.objects.missingMessages(extend({
      live: true,
      gte: start
    }, getFromTo(opts)))

    stream.once('data', function (seq) {
      // we know we have message seq - 1
      // cause seq is the first one missing
      checkpoint = seq - 1
    })

    stream.on('data', onMissing)
    stream.on('error', err => ee.emit('error', err))
  }

  ee.stop = function stop () {
    ee.emit('stop')
    if (stream) stream.end()
  }

  ee.reset(checkpoint)
  return ee

  function onMissing (seq) {
    if (typeof tip === 'undefined') {
      ee.once('tip', () => onMissing(seq))
      return
    }

    ee.emit('missing', { seq, tip })
  }

  function onNewTip (err, newTip) {
    if (err) return ee.emit('error')

    tip = newTip
    ee.emit('tip', tip)
  }
}

conversation.monitorTip = function monitorTip (opts) {
  const { node, counterparty, outbound, onChange } = opts
  const fromTo = getFromTo(opts)
  co(function* () {
    // get current tip
    const streamOpts = extend({
      limit: 1,
      reverse: true
    }, fromTo)

    let seq
    try {
      const results = yield collect(node.objects.seq(streamOpts))
      seq = results.length ? results[0] : -1
    } catch (err) {
      debug('1. seq stream failed', err)
      return onChange(err)
    }

    onChange(null, seq)
    monitor(seq)
  })()

  function monitor (afterSeq) {
    node.objects.seq(extend({
        live: true,
        gt: afterSeq
      }, fromTo))
      .on('data', seq => onChange(null, seq))
      .on('error', function (err) {
        debug('2. seq stream failed', err)
        onChange(err)
      })
  }
}

function batchifyMonitor ({ monitor, debounce=1000 }) {
  const batch = []
  let fireTimeout
  let latestTip

  monitor.on('missing', function addMissing ({ seq, tip }) {
    latestTip = tip
    batch.push(seq)
    clearTimeout(fireTimeout)
    fireTimeout = setTimeout(function () {
      const copy = batch.slice()
      batch.length = 0
      monitor.emit('batch', {
        tip: latestTip,
        seqs: copy
      })
    }, debounce)
  })

  monitor.on('stop', function () {
    clearTimeout(fireTimeout)
    batch.length = 0
  })

  return monitor
}

function streamSent (opts) {
  const { node, from, to } = opts
  const seqOpts = omit(opts, 'node', 'from', 'to')
  const base = from + INDEX_SEPARATOR + to
  seqOpts.gte = base + INDEX_SEPARATOR
  seqOpts.lte = base + '\xff'
  const source = node.objects.bySeq(seqOpts)

  let ended
  return pump(
    source,
    through.obj(function (data, enc, cb) {
      if (ended) return cb()
      if (data.recipient === to) {
        if (data.sendstatus === Status.send.pending) {
          debug('reached unsent message, terminating stream')
          ended = true
          source.end()
          return cb()
        }

        cb(null, data)
      } else {
        cb()
      }
    })
  )
}

function xor (a, b) {
  return a || b && !(a && b)
}

function getFromTo ({ node, counterparty, outbound }) {
  return outbound
    ? { from: node.permalink, to: counterparty }
    : { from: counterparty, to: node.permalink}
}

module.exports = {
  conversation,
  batchifyMonitor
}

// exports.missing = co(function* ({ node, from }) {
//   return collect(node.objects.missingMessages({ from }))
// })
