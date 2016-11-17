_         = require 'lodash'
JobLogger = require 'job-logger'
RedisNS   = require '@octoblu/redis-ns'
Redis     = require 'ioredis'
Worker    = require './worker'

class WorkerRunner
  constructor: (options) ->
    {@redisUri,@concurrency} = options
    {@privateKey,@requestTimeout} = options
    {@jobLogRedisUri,@jobLogQueue,@jobLogSampleRate} = options
    {@queueName,@queueTimeout} = options
    {@meshbluConfig,@namespace} = options
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

  stop: (callback) =>
    @worker?.stop?(callback)

  run: (callback) =>
    @getWorkerClient (error, client) =>
      return callback error if error?
      @getJobLogger (error, jobLogger) =>
        return callback error if error?
        @worker = new Worker {
          client,
          jobLogger,
          @queueName,
          @queueTimeout,
          @requestTimeout,
          @privateKey,
          @meshbluConfig,
          @jobLogSampleRate,
          @concurrency,
        }
        @worker.run callback

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

  getWorkerClient: (callback) =>
    @getRedisClient @redisUri, (error, client) =>
      return callback error if error?
      clientNS  = new RedisNS @namespace, client
      callback null, clientNS

  getRedisClient: (redisUri, callback) =>
    callback = _.once callback
    client = new Redis redisUri, dropBufferSupport: true
    client = _.bindAll client, _.functionsIn(client)
    client.once 'ready', =>
      callback null, client
    client.once 'error', callback

module.exports = WorkerRunner
