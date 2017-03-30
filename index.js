const { EventEmitter } = require('events')
const Promise = require('bluebird')
const co = Promise.coroutine
const collect = Promise.promisify(require('stream-collector'))
const through = require('through2')
const pump = require('pump')
const omit = require('object.omit')
const debug = require('debug')('tradle:restore')
const { typeforce, utils, constants } = require('@tradle/engine')
const { TYPE, INDEX_SEPARATOR } = constants
const RESTORE_REQUEST = 'tradle.RestoreRequest'
const addAuthor = Promise.promisify(utils.addAuthor)
const conversation = {}

conversation.respond = co(function* ({ node, req, sent, received }) {
  // TODO: support ranges, optimize
  if (!xor(sent, received)) {
    throw new Error('expected "sent" OR "received"')
  }

  const validate = Promise.promisify(node.validator.validate)
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

  const { seqs } = req
  const msgs = pump(
    streamSent({  node, from, to, body: false }),
    through.obj(function (data, enc, cb) {
      const { value } = data
      const { seq, link } = data
      if (seqs.indexOf(seq) === -1) return cb()

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

conversation.request = co(function* ({ node, from, seqs }) {
  const sign = Promise.promisify(node.sign.bind(node))
  return sign({
    object: {
      [TYPE]: RESTORE_REQUEST,
      from,
      to: node.permalink,
      seqs
    }
  })
})

conversation.monitorMissing = function monitorMissing (opts) {
  typeforce({
    node: 'Object',
    counterparty: 'String'
  }, opts)

  let { node, counterparty, checkpoint=0 } = opts
  const { objects } = node
  let stream

  const ee = new EventEmitter()
  ee.reset = function reset (start=checkpoint) {
    checkpoint = start
    ee.stop()

    stream = node.objects.missingMessages({
      from: counterparty,
      live: true,
      gte: start
    })

    stream.on('data', seq => ee.emit('missing', seq))
    stream.once('data', function (seq) {
      // we know we have message seq - 1
      // cause seq is the first one missing
      checkpoint = seq - 1
    })

    stream.on('error', err => ee.emit('error', err))
  }

  ee.stop = function stop () {
    ee.emit('stop')
    if (stream) stream.end()
  }

  ee.reset(checkpoint)
  return ee
}

function batchifyMonitor ({ monitor, debounce=1000 }) {
  const batch = []
  let fireTimeout

  monitor.on('missing', function addMissing (seq) {
    batch.push(seq)
    clearTimeout(fireTimeout)
    fireTimeout = setTimeout(function () {
      const copy = batch.slice()
      batch.length = 0
      monitor.emit('batch', copy)
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
  return pump(
    node.objects.seq(seqOpts),
    through.obj(function (data, enc, cb) {
      if (data.recipient === to) {
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

module.exports = {
  conversation,
  streamSent,
  batchifyMonitor
}

// exports.missing = co(function* ({ node, from }) {
//   return collect(node.objects.missingMessages({ from }))
// })
