_              = require 'lodash'
chalk          = require 'chalk'
dashdash       = require 'dashdash'
Redis          = require 'ioredis'
MeshbluConfig  = require 'meshblu-config'
JobLogger      = require 'job-logger'
RedisNS        = require '@octoblu/redis-ns'
Worker         = require './src/worker'
SigtermHandler = require 'sigterm-handler'

packageJSON = require './package.json'

OPTIONS = [
  {
    names: ['redis-uri', 'r']
    type: 'string'
    env: 'REDIS_URI'
    help: 'Redis URI'
  }
  {
    names: ['redis-namespace', 'n']
    type: 'string'
    env: 'REDIS_NAMESPACE'
    default: 'meshblu-webhooks'
    help: 'Redis namespace for redis-ns'
  }
  {
    name: 'job-log-redis-uri'
    type: 'string'
    help: 'URI for job log Redis'
    env: 'JOB_LOG_REDIS_URI'
  }
  {
    name: 'job-log-queue'
    type: 'string'
    help: 'Job log queue name'
    env: 'JOB_LOG_QUEUE'
  }
  {
    name: 'job-log-sample-rate'
    type: 'number'
    help: 'Job log sample rate (0.00 to 1.00)'
    env: 'JOB_LOG_SAMPLE_RATE'
  }
  {
    names: ['queue-name', 'q']
    type: 'string'
    env: 'QUEUE_NAME'
    default: 'webhooks'
    help: 'Name of Redis work queue'
  },
  {
    names: ['queue-timeout']
    type: 'positiveInteger'
    env: 'QUEUE_TIMEOUT'
    default: 30
    help: 'BRPOP timeout (in seconds)'
  },
  {
    names: ['request-timeout']
    type: 'positiveInteger'
    env: 'REQUEST_TIMEOUT'
    default: 15
    help: 'Request timeout (in seconds)'
  },
  {
    name: 'private-key-base64'
    type: 'string'
    help: 'Base64-encoded private key'
    env: 'PRIVATE_KEY_BASE64'
  },
  {
    names: ['help', 'h']
    type: 'bool'
    help: 'Print this help and exit.'
  },
  {
    names: ['version', 'v']
    type: 'bool'
    help: 'Print the version and exit.'
  }
]

class Command
  constructor: ->
    process.on 'uncaughtException', @die
    {
      @mongodb_uri
      @redis_uri
      @redis_namespace
      @queue_timeout
      @request_timeout
      @queue_name
      @privateKey
      @job_log_redis_uri
      @job_log_queue
      @job_log_sample_rate
    } = @parseOptions()

  parseOptions: =>
    parser = dashdash.createParser({options: OPTIONS})
    options = parser.parse(process.argv)

    if options.help
      console.log "usage: meshblu-core-worker-webhook [OPTIONS]\noptions:\n#{parser.help({includeEnv: true, includeDefaults: true})}"
      process.exit 0

    if options.version
      console.log packageJSON.version
      process.exit 0

    unless options.redis_uri? && options.redis_namespace? && options.queue_name? && options.queue_timeout?
      console.error "usage: meshblu-core-worker-webhook [OPTIONS]\noptions:\n#{parser.help({includeEnv: true, includeDefaults: true})}"
      console.error chalk.red 'Missing required parameter --redis-uri, -r, or env: REDIS_URI' unless options.redis_uri?
      console.error chalk.red 'Missing required parameter --redis-namespace, -n, or env: REDIS_NAMESPACE' unless options.redis_namespace?
      console.error chalk.red 'Missing required parameter --queue-timeout, -t, or env: QUEUE_TIMEOUT' unless options.queue_timeout?
      console.error chalk.red 'Missing required parameter --queue-name, -u, or env: QUEUE_NAME' unless options.queue_name?
      process.exit 1

    unless options.private_key_base64?
      console.error "usage: meshblu-core-worker-webhook [OPTIONS]\noptions:\n#{parser.help({includeEnv: true, includeDefaults: true})}"
      console.error chalk.red 'Missing required parameter --private-key-base64, or env: PRIVATE_KEY_BASE64' unless options.private_key_base64?
      process.exit 1

    unless options.job_log_redis_uri? && options.job_log_queue? && options.job_log_sample_rate?
      console.error "usage: meshblu-core-worker-webhook [OPTIONS]\noptions:\n#{parser.help({includeEnv: true, includeDefaults: true})}"
      console.error chalk.red 'Missing required parameter --job-log-redis-uri, or env: JOB_LOG_REDIS_URI' unless options.job_log_redis_uri?
      console.error chalk.red 'Missing required parameter --job-log-queue, or env: JOB_LOG_QUEUE' unless options.job_log_queue?
      console.error chalk.red 'Missing required parameter --job-log-sample-rate, or env: JOB_LOG_SAMPLE_RATE' unless options.job_log_sample_rate?
      process.exit 1

    options.privateKey = new Buffer(options.private_key_base64, 'base64').toString('utf8')

    return options

  run: =>
    meshbluConfig = new MeshbluConfig().toJSON()

    @getWorkerClient (error, client) =>
      return @die error if error?
      @getJobLogger (error, jobLogger) =>
        return @die error if error?
        worker = new Worker {
          client,
          jobLogger,
          jobLogSampleRate: @job_log_sample_rate,
          queueName: @queue_name,
          queueTimeout: @queue_timeout,
          requestTimeout: @request_timeout,
          @privateKey,
          meshbluConfig
        }

        worker.run @die

        sigtermHandler = new SigtermHandler { events: ['SIGINT', 'SIGTERM']}
        sigtermHandler.register worker.stop

  getJobLogger: (callback) =>
    @getRedisClient @job_log_redis_uri, (error, client) =>
      return callback error if error?
      jobLogger = new JobLogger {
        client,
        indexPrefix: 'metric:meshblu-core-worker-webhook'
        type: 'meshblu-core-worker-webhook:job'
        jobLogQueue: @job_log_queue
      }
      callback null, jobLogger

  getWorkerClient: (callback) =>
    @getRedisClient @redis_uri, (error, client) =>
      return callback error if error?
      clientNS  = new RedisNS @redis_namespace, client
      callback null, clientNS

  getRedisClient: (redisUri, callback) =>
    callback = _.once callback
    client = new Redis redisUri, dropBufferSupport: true
    client.once 'ready', =>
      client.on 'error', @die
      callback null, client

    client.once 'error', callback

  die: (error) =>
    return process.exit(0) unless error?
    console.error 'ERROR'
    console.error error.stack
    process.exit 1

module.exports = Command
