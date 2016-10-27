_               = require 'lodash'
request         = require 'request'
async           = require 'async'
MeshbluHttp     = require 'meshblu-http'
SimpleBenchmark = require 'simple-benchmark'
debug           = require('debug')('meshblu-core-worker-webhook:worker')

class Worker
  constructor: (options={})->
    { @privateKey, @meshbluConfig } = options
    { @client, @queueName, @queueTimeout, @logFn } = options
    { @jobLogger, @jobLogSampleRate } = options
    { @requestTimeout } = options
    throw new Error('Worker: requires client') unless @client?
    throw new Error('Worker: requires jobLogger') unless @jobLogger?
    throw new Error('Worker: requires jobLogSampleRate') unless @jobLogSampleRate?
    throw new Error('Worker: requires queueName') unless @queueName?
    throw new Error('Worker: requires queueTimeout') unless @queueTimeout?
    throw new Error('Worker: requires privateKey') unless @privateKey?
    throw new Error('Worker: requires meshbluConfig') unless @meshbluConfig?
    throw new Error('Worker: requires requestTimeout') unless @requestTimeout?
    delete @meshbluConfig.uuid
    delete @meshbluConfig.token
    debug 'using meshblu config', @meshbluConfig
    @logFn ?= console.error
    @shouldStop = false
    @isStopped = false

  doWithNextTick: (callback) =>
    # give some time for garbage collection
    process.nextTick =>
      @do (error) =>
        process.nextTick =>
          callback error

  do: (callback) =>
    @client.brpop @queueName, @queueTimeout, (error, result) =>
      return callback error if error?
      return callback() unless result?

      [ queue, jobRequest ] = result
      try
        jobRequest = JSON.parse jobRequest
      catch error
        return callback error

      jobBenchmark = new SimpleBenchmark { label: 'meshblu-core-worker-webhook:job' }
      @_process jobRequest, (error, jobResponse) =>
        @logFn error.stack if error?
        @_logJob { error, jobBenchmark, jobResponse, jobRequest }, (error) =>
          @logFn error.stack if error?
          callback()

    return # avoid returning promise

  run: (callback) =>
    async.doUntil @doWithNextTick, (=> @shouldStop), =>
      debug('STOPPED!')
      callback()

  stop: (callback) =>
    debug('STOPPING...')
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
    signRequest ?= false
    @_request { options: requestOptions, signRequest }, (requestError, jobResponse) =>
      requestError?.type = 'RequestError'
      return callback requestError, jobResponse if signRequest
      return callback requestError, jobResponse unless revokeOptions?.token?
      @_revoke revokeOptions, (revokeError) =>
        revokeError?.type = 'RevokeError'
        error = requestError ? revokeError ? null
        callback error, jobResponse

  _revoke: ({ uuid, token }, callback) =>
    _meshbluConfig = _.cloneDeep @meshbluConfig
    _meshbluConfig.uuid = uuid
    _meshbluConfig.token = token
    _meshbluConfig.timeout = @requestTimeout * 1000
    meshbluHttp = new MeshbluHttp _meshbluConfig
    debug 'revoking', { uuid, token, timeout: _meshbluConfig.timeout }
    meshbluHttp.revokeToken uuid, token, (error) =>
      return callback error if error?
      callback null

  _request: ({ options, signRequest }, callback) =>
    debug 'request.options', options
    debug 'request.signRequest', signRequest
    options.httpSignature = @_createSignatureOptions() if signRequest
    options.timeout = @requestTimeout * 1000
    request options, (error, response) =>
      return callback error if error?
      debug 'response.code', response.statusCode
      callback null, response

  _createSignatureOptions: =>
    return {
      keyId: 'meshblu-webhook-key'
      key: @privateKey
      headers: [ 'date', 'X-MESHBLU-UUID' ]
    }

  _getJobLogs: =>
    jobLogs = []
    if Math.random() < @jobLogSampleRate
      jobLogs.push 'sampled'
    return jobLogs

  _formatRequestLog: ({ requestOptions, revokeOptions, signRequest }, url) =>
    return {
      metadata: {
        signRequest: signRequest || false,
        url: url
        revokeOptions: {
          uuid: revokeOptions?.uuid
          hasToken: revokeOptions?.token?
        }
      }
    }

  _formatErrorLog: (error, url) =>
    code = error?.code ? 500
    code = 408 if code == 'ETIMEDOUT'
    return {
      metadata:
        code: code
        success: false
        jobLogs: @_getJobLogs()
        url: url
        error:
          type: error?.type ? 'Unknown Type'
          message: error?.message ? 'Unknown Error'
    }

  _formatResponseLog: (jobResponse, url) =>
    code = _.get(jobResponse, 'statusCode') ? 500
    debug 'code', code
    return {
      metadata: {
        code: code
        success: code > 399
        url: url
        jobLogs: @_getJobLogs()
      }
    }

  _logJob: ({ error, jobRequest, jobResponse, jobBenchmark }, callback) =>
    url = _.get(jobRequest, 'requestOptions.url')
    _request = @_formatRequestLog jobRequest, url
    _response = @_formatResponseLog jobResponse, url
    _response = @_formatErrorLog error, url if error?
    debug '_logJob', _request, _response
    @jobLogger.log {request:_request, response:_response, elapsedTime: jobBenchmark.elapsed()}, callback

module.exports = Worker
