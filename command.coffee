_              = require 'lodash'
chalk          = require 'chalk'
dashdash       = require 'dashdash'
MeshbluConfig  = require 'meshblu-config'
WorkerRunner   = require './src/worker-runner'
SigtermHandler = require 'sigterm-handler'
OctobluRaven   = require 'octoblu-raven'
packageJSON    = require './package.json'

OPTIONS = [
  {
    name: 'redis-uri'
    type: 'string'
    env: 'REDIS_URI'
    help: 'Redis URI'
  }
  {
    name: 'namespace'
    type: 'string'
    env: 'NAMESPACE'
    help: 'Redis namespace for redis-ns'
    default: 'meshblu-webhooks'
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
    default: 'sample-rate:1.00'
  }
  {
    name: 'job-log-sample-rate'
    type: 'number'
    help: 'Job log sample rate (0.00 to 1.00)'
    env: 'JOB_LOG_SAMPLE_RATE'
    default: 0
  }
  {
    name: 'queue-name'
    type: 'string'
    env: 'QUEUE_NAME'
    help: 'Name of Redis work queue'
    default: 'webhooks'
  },
  {
    name: 'queue-timeout'
    type: 'positiveInteger'
    env: 'QUEUE_TIMEOUT'
    help: 'BRPOP timeout (in seconds)'
    default: 30
  },
  {
    name: 'request-timeout'
    type: 'positiveInteger'
    env: 'REQUEST_TIMEOUT'
    help: 'Request timeout (in seconds)'
    default: 15
  },
  {
    name: 'concurrency'
    type: 'positiveInteger'
    env: 'WORK_CONCURRENCY'
    help: 'Number of jobs to process at a time'
    default: 5
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
    {
      @redis_uri
      @namespace
      @queue_timeout
      @request_timeout
      @queue_name
      @privateKey
      @job_log_redis_uri
      @job_log_queue
      @job_log_sample_rate
      @concurrency
    } = @parseOptions()
    @meshbluConfig = new MeshbluConfig().toJSON()
    @octobluRaven = new OctobluRaven { release: packageJSON.version }
    @octobluRaven.patchGlobal()

  parseOptions: =>
    parser = dashdash.createParser({options: OPTIONS})
    options = parser.parse(process.argv)

    if options.help
      console.log "usage: meshblu-core-worker-webhook [OPTIONS]\noptions:\n#{parser.help({includeEnv: true, includeDefaults: true})}"
      process.exit 0

    if options.version
      console.log packageJSON.version
      process.exit 0

    options.namespace ?= process.env.REDIS_NAMESPACE
    options.job_log_redis_uri ?= options.redis_uri

    unless options.redis_uri? && options.namespace? && options.queue_name? && options.queue_timeout?
      console.error "usage: meshblu-core-worker-webhook [OPTIONS]\noptions:\n#{parser.help({includeEnv: true, includeDefaults: true})}"
      console.error chalk.red 'Missing required parameter --redis-uri, -r, or env: REDIS_URI' unless options.redis_uri?
      console.error chalk.red 'Missing required parameter --namespace, -n, or env: NAMESPACE' unless options.namespace?
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
    @octobluRaven = new OctobluRaven { release: packageJSON.version }
    workerRunner = new WorkerRunner {
      redisUri: @redis_uri,
      jobLogRedisUri: @job_log_redis_uri ? @redis_uri,
      jobLogQueue: @job_log_queue,
      jobLogSampleRate: @job_log_sample_rate,
      queueName: @queue_name,
      queueTimeout: @queue_timeout,
      requestTimeout: @request_timeout,
      @concurrency,
      @namespace,
      @privateKey,
      @meshbluConfig,
      @octobluRaven
    }
    workerRunner.start (error) =>
      @die error if error?
    sigtermHandler = new SigtermHandler { events: ['SIGINT', 'SIGTERM'] }
    sigtermHandler.register workerRunner.stop

  die: (error) =>
    return process.exit(0) unless error?
    @octobluRaven.reportError arguments...
    console.error 'ERROR'
    console.error error.stack
    process.exit 1

module.exports = Command
