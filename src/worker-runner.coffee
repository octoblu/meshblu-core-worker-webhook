_            = require 'lodash'
JobLogger    = require 'job-logger'
RedisNS      = require '@octoblu/redis-ns'
Redis        = require 'ioredis'
OctobluRaven = require 'octoblu-raven'
Worker       = require './worker'
packageJSON  = require '../package.json'

class WorkerRunner
  constructor: (options) ->
    {@redisUri,@concurrency} = options
    {@privateKey,@requestTimeout} = options
    {@jobLogRedisUri,@jobLogQueue,@jobLogSampleRate} = options
    {@queueName,@queueTimeout} = options
    {@meshbluConfig,@namespace} = options
    {@octobluRaven} = options
    throw new Error 'WorkerRunner: requires redisUri' unless @redisUri?
    throw new Error 'WorkerRunner: requires jobLogRedisUri' unless @jobLogRedisUri?
    throw new Error 'WorkerRunner: requires jobLogQueue' unless @jobLogQueue?
    throw new Error 'WorkerRunner: requires jobLogSampleRate' unless @jobLogSampleRate?
    throw new Error 'WorkerRunner: requires queueName' unless @queueName?
    throw new Error 'WorkerRunner: requires queueTimeout' unless @queueTimeout?
    throw new Error 'WorkerRunner: requires requestTimeout' unless @requestTimeout?
    throw new Error 'WorkerRunner: requires privateKey' unless @privateKey?
    throw new Error 'WorkerRunner: requires meshbluConfig' unless @meshbluConfig?
    throw new Error 'WorkerRunner: requires namespace' unless @namespace?
    @octobluRaven ?= new OctobluRaven { release: packageJSON.version }

  stop: (callback) =>
    return callback new Error 'worker has not yet started' unless @worker?
    @worker.stop callback

  start: (callback) =>
    @getJobLogger (error, jobLogger) =>
      return callback error if error?
      @worker = new Worker {
        jobLogger
        @queueName
        @queueTimeout
        @requestTimeout
        @privateKey
        @meshbluConfig
        @jobLogSampleRate
        @concurrency
        @octobluRaven
        @namespace
        @redisUri
      }
      @worker.start callback

  getJobLogger: (callback) =>
    indexPrefix = 'metric:meshblu-core-worker-webhook'
    type = 'meshblu-core-worker-webhook:job'
    @getRedisClient @jobLogRedisUri, (error, client) =>
      return callback error if error?
      jobLogger = new JobLogger {
        client,
        @jobLogQueue
        indexPrefix
        type
      }
      callback null, jobLogger

  getRedisClient: (redisUri, callback) =>
    callback = _.once callback
    client = new Redis redisUri, { dropBufferSupport: true }
    client = _.bindAll client, _.functionsIn(client)
    client.ping (error) =>
      return callback error if error?
      client.once 'error', @dieWithError
      callback null, client

  dieWithError: (error) =>
    @octobluRaven.reportError arguments...
    console.error error.stack
    process.exit 1

module.exports = WorkerRunner
