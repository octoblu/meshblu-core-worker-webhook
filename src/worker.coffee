request = require 'request'
async   = require 'async'
debug   = require('debug')('meshblu-core-webhook-worker:worker')

class Worker
  constructor: (options={})->
    { @redis, @queueName, @queueTimeout, @privateKey } = options
    throw new Error('Worker: requires redis') unless @redis?
    throw new Error('Worker: requires queueName') unless @queueName?
    throw new Error('Worker: requires queueTimeout') unless @queueTimeout?
    throw new Error('Worker: requires privateKey') unless @privateKey?
    @shouldStop = false
    @isStopped = false

  do: (callback) =>
    @redis.brpop @queueName, @queueTimeout, (error, result) =>
      return callback error if error?
      return callback() unless result?

      [ queue, data ] = result
      try
        data = JSON.parse data
      catch error
        return callback error

      @_process data, (error) =>
        console.error error.stack if error?
        callback()

    return # avoid returning promise

  run: =>
    async.doUntil @do, (=> @shouldStop), =>
      @isStopped = true

  stop: (callback) =>
    @shouldStop = true

    timeout = setTimeout =>
      clearInterval interval
      callback new Error 'Stop Timeout Expired'
    , 5000

    interval = setInterval =>
      return unless @isStopped?
      clearInterval interval
      clearTimeout timeout
      callback()
    , 250

  _process: ({ requestOptions, revokeOptions, signRequest }, callback) =>
    @_request { options: requestOptions, signRequest }, (requestError) =>
      @_request { options: revokeOptions }, (revokeError) =>
        callback(requestError ? revokeError ? null)

  _request: ({ options, signRequest }, callback) =>
    debug 'request.options', options
    debug 'request.signRequest', signRequest
    options.httpSignature = @_createSignatureOptions() if signRequest
    request options, (error) =>
      return callback error if error?
      callback null

  _createSignatureOptions: =>
    return {
      keyId: 'meshblu-webhook-key'
      key: @privateKey
      headers: [ 'date', 'X-MESHBLU-UUID' ]
    }

module.exports = Worker
