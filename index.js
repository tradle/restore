
const Promise = require('bluebird')
const co = Promise.coroutine
const collect = Promise.promisify(require('stream-collector'))
const through = require('through2')
const pump = require('pump')
const debug = require('debug')('tradle:restore')
const { utils, constants } = require('@tradle/engine')
const { TYPE } = constants
const RESTORE_REQUEST = 'tradle.RestoreRequest'
const addAuthor = Promise.promisify(utils.addAuthor)
const conversation = {}

exports.conversation = conversation

conversation.respond = co(function* ({ node, req }) {
  // TODO: support ranges, optimize

  const validate = Promise.promisify(node.validator.validate)
  // only restore for original conversation participant
  const { from, to } = req
  const me = node.permalink
  if (me !== from && me !== to) {
    throw new Error('expected self to be a conversation participant')
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
    node.conversation({ with: them, body: false }),
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

// exports.missing = co(function* ({ node, from }) {
//   return collect(node.objects.missingMessages({ from }))
// })
