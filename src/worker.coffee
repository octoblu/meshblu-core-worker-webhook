_                               = require 'lodash'
request                         = require 'request'
validator                       = require 'validator'
async                           = require 'async'
URL                             = require 'url'
MeshbluHttp                     = require 'meshblu-http'
SimpleBenchmark                 = require 'simple-benchmark'
OctobluRaven                    = require 'octoblu-raven'
{STATUS_CODES}                  = require 'http'
packageJSON                     = require '../package.json'
debug                           = require('debug')('meshblu-core-worker-webhook:worker')
{ JobManagerResponderDequeuer } = require 'meshblu-core-job-manager'
GenericPool                     = require 'generic-pool'
Redis                           = require 'ioredis'
RedisNS                         = require '@octoblu/redis-ns'
When                            = require 'when'

class Worker
  constructor: (options={})->
    {
      @privateKey
      @meshbluConfig
      @queueName
      @queueTimeout
      @consoleError
      @jobLogger
      @jobLogSampleRate
      @requestTimeout
      concurrency
      @octobluRaven
      @namespace
      @redisUri
    } = options
    throw new Error('Worker: requires jobLogger') unless @jobLogger?
    throw new Error('Worker: requires jobLogSampleRate') unless @jobLogSampleRate?
    throw new Error('Worker: requires queueName') unless @queueName?
    throw new Error('Worker: requires queueTimeout') unless @queueTimeout?
    throw new Error('Worker: requires privateKey') unless @privateKey?
    throw new Error('Worker: requires meshbluConfig') unless @meshbluConfig?
    throw new Error('Worker: requires requestTimeout') unless @requestTimeout?
    throw new Error('Worker: requires namespace') unless @namespace?
    throw new Error('Worker: requires redisUri') unless @redisUri?
    delete @meshbluConfig.uuid
    delete @meshbluConfig.token
    @octobluRaven ?= new OctobluRaven { release: packageJSON.version }
    @requestTimeout = @requestTimeout * 1000
    debug 'request timeout', @requestTimeout
    @consoleError ?= @_consoleError
    @_shouldStop = false
    concurrency ?= 1
    @maxConnections ?= concurrency
    @minConnections ?= 1
    @idleTimeoutMillis ?= 60000
    debug 'concurrency', concurrency
    @_queuePool = @_createRedisPool { @maxConnections, @minConnections, @idleTimeoutMillis, @namespace, @redisUri }
    @queue = async.queue @doTask, concurrency
    @dequeuers = []
    _.times Math.ceil(concurrency/3), (x) =>
      @dequeuers.push new JobManagerResponderDequeuer {
        x
        @queue
        @_queuePool
        @onPush
        _updateHeartbeat: _.noop
        requestQueueName: @queueName
        queueTimeoutSeconds: @queueTimeout
      }

  _consoleError: (key, error, metadata) =>
    _.set error, 'reason', key
    _.set error, 'metadata', metadata if metadata?
    return if _.get(error, 'reportError', true)
    @octobluRaven.reportError error
    debug 'got error', key, error

  doTask: (rawData, callback) =>
    debug 'doTask', {rawData}
    process.nextTick =>
      try
        jobRequest = JSON.parse rawData
      catch error
        @consoleError 'Unable to parse', jobRequest
        callback error
        return
      jobBenchmark = new SimpleBenchmark { label: 'meshblu-core-worker-webhook:job' }
      @_process jobRequest, (error, jobResponse) =>
        @consoleError 'Process Error', error, {jobRequest} if error?
        @_logJob { error, jobBenchmark, jobResponse, jobRequest }, (error) =>
          @consoleError 'Log Job Error', error, {jobResponse,jobRequest} if error?
          callback()
    return # avoid returning promise

  onPush: =>
    debug 'onPush'
    @_drained = false

  start: (callback) =>
    @_drained = true
    @queue.drain = =>
      debug 'drained'
      @_drained = true
    tasks = []
    _.each @dequeuers, (dequeuer) =>
      tasks.push dequeuer.start
    async.parallel tasks, callback

  stop: (callback=_.noop) =>
    tasks = []
    _.each @dequeuers, (dequeuer) =>
      tasks.push dequeuer.stop
    async.parallel tasks, =>
      async.doUntil @_waitForStopped, (=> @_drained), callback

  _waitForStopped: (callback) =>
    _.delay callback, 100

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
    _meshbluConfig.timeout = @requestTimeout
    meshbluHttp = new MeshbluHttp _meshbluConfig
    debug 'revoking', { uuid, token, timeout: _meshbluConfig.timeout }
    meshbluHttp.revokeToken uuid, token, (error) =>
      return callback error if error?
      callback null

  _request: ({ options, signRequest }, callback) =>
    debug 'request.options', options
    debug 'request.signRequest', signRequest
    options.httpSignature = @_createSignatureOptions() if signRequest
    options.timeout = @requestTimeout
    options.forever = false
    @validateURL options, (error) =>
      return callback error if error?
      request options, (error, response) =>
        return callback error if error?
        debug 'response.code', response.statusCode
        callback null, response

  validateURL: ({ url, baseUrl, uri }, callback) =>
    if _.isString url
      urlParts = URL.parse url
    else if _.isString baseUrl
      urlParts = URL.parse baseUrl
      urlParts.pathname = uri if _.isString(uri)
    else
      return callback @_validationError(urlParts)
    if _.endsWith urlParts.hostname, 'undefined'
      return callback @_validationError(urlParts)
    if _.endsWith urlParts.hostname, 'null'
      return callback @_validationError(urlParts)
    unless validator.isURL(URL.format(urlParts), require_tld: false)
      return callback @_validationError(urlParts)
    callback null

  _validationError: (urlParts) =>
    error = new Error 'Invalid URL'
    error.code = 422
    error.reportError = false
    error.urlParts = urlParts
    return error

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
      metadata:
        signRequest: signRequest || false,
        taskName: url
        jobType: 'webhook'
        auth:
          uuid: revokeOptions?.uuid
        revokeOptions:
          uuid: revokeOptions?.uuid
          hasToken: revokeOptions?.token?
    }

  _formatErrorLog: (error, url) =>
    code = @_sanifyCode error?.code
    return {
      metadata:
        code: code
        success: false
        jobLogs: @_getJobLogs()
        jobType: 'webhook'
        taskName: url
        error:
          type: error?.type ? 'Unknown Type'
          message: error?.message ? 'Unknown Error'
    }

  _formatResponseLog: (jobResponse, url) =>
    code = @_sanifyCode _.get(jobResponse, 'statusCode')
    debug 'code', code
    return {
      metadata:
        code: code
        success: code > 399
        taskName: url
        jobType: 'webhook'
        jobLogs: @_getJobLogs()
    }

  _sanifyCode: (code) =>
    return code if STATUS_CODES[code]?
    return 408 if code == 'ETIMEDOUT'
    return 408 if code == 'ESOCKETTIMEDOUT'
    return 500

  _logJob: ({ error, jobRequest, jobResponse, jobBenchmark }, callback) =>
    url = _.get(jobRequest, 'requestOptions.url')
    _request = @_formatRequestLog jobRequest, url
    _response = @_formatResponseLog jobResponse, url
    _response = @_formatErrorLog error, url if error?
    debug '_logJob', _request, _response
    @jobLogger.log {request:_request, response:_response, elapsedTime: jobBenchmark.elapsed()}, callback

  _createRedisPool: ({ maxConnections, minConnections, idleTimeoutMillis, evictionRunIntervalMillis, acquireTimeoutMillis, namespace, redisUri }) =>
    factory =
      create: =>
        return When.promise (resolve, reject) =>
          conx = new Redis redisUri, dropBufferSupport: true
          client = new RedisNS namespace, conx
          rejectError = (error) =>
            return reject error

          client.once 'error', rejectError
          client.once 'ready', =>
            client.removeListener 'error', rejectError
            resolve client

      destroy: (client) =>
        return When.promise (resolve, reject) =>
          @_closeClient client, (error) =>
            return reject error if error?
            resolve()

      validate: (client) =>
        return When.promise (resolve) =>
          client.ping (error) =>
            return resolve false if error?
            resolve true

    options = {
      max: maxConnections
      min: minConnections
      testOnBorrow: true
      idleTimeoutMillis
      evictionRunIntervalMillis
      acquireTimeoutMillis
    }

    pool = GenericPool.createPool factory, options

    pool.on 'factoryCreateError', (error) =>
      @emit 'factoryCreateError', error

    return pool

module.exports = Worker
