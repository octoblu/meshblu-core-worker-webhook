chalk    = require 'chalk'
dashdash = require 'dashdash'
Redis    = require 'ioredis'
RedisNS  = require '@octoblu/redis-ns'
Worker   = require './src/worker'

packageJSON = require './package.json'

OPTIONS = [
  {
    names: ['redis-uri', 'r']
    type: 'string'
    env: 'REDIS_URI'
    help: 'Redis URI'
  },
  {
    names: ['redis-namespace', 'n']
    type: 'string'
    env: 'REDIS_NAMESPACE'
    default: 'meshblu-webhooks'
    help: 'Redis namespace for redis-ns'
  },
  {
    names: ['queue-name', 'q']
    type: 'string'
    env: 'QUEUE_NAME'
    default: 'webhooks'
    help: 'Name of Redis work queue'
  },
  {
    names: ['queue-timeout', 't']
    type: 'positiveInteger'
    env: 'QUEUE_TIMEOUT'
    default: 30
    help: 'BRPOP timeout (in seconds)'
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
      @queue_name
    } = @parseOptions()

  parseOptions: =>
    parser = dashdash.createParser({options: OPTIONS})
    options = parser.parse(process.argv)

    if options.help
      console.log "usage: meshblu-core-webhook-worker [OPTIONS]\noptions:\n#{parser.help({includeEnv: true})}"
      process.exit 0

    if options.version
      console.log packageJSON.version
      process.exit 0

    unless options.mongodb_uri? && options.redis_uri? && options.redis_namespace? && options.queue_name? && options.queue_timeout?
      console.error "usage: meshblu-core-webhook-worker [OPTIONS]\noptions:\n#{parser.help({includeEnv: true})}"
      console.error chalk.red 'Missing required parameter --mongodb-uri, -m, or env: MONGODB_URI' unless options.mongodb_uri?
      console.error chalk.red 'Missing required parameter --redis-uri, -r, or env: REDIS_URI' unless options.redis_uri?
      console.error chalk.red 'Missing required parameter --redis-namespace, -n, or env: REDIS_NAMESPACE' unless options.redis_namespace?
      console.error chalk.red 'Missing required parameter --queue-timeout, -t, or env: QUEUE_TIMEOUT' unless options.queue_timeout?
      console.error chalk.red 'Missing required parameter --queue-name, -u, or env: QUEUE_NAME' unless options.queue_name?
      process.exit 1

    return options

  run: =>
    db = mongojs @mongodb_uri, ['deployments', 'ci-builds', 'docker-builds']
    client = new Redis @redis_uri, dropBufferSupport: true
    redis = new RedisNS @redis_namespace, client

    client.on 'ready', =>
      worker = new Worker { db, redis, queueName: @queue_name, queueTimeout: @queue_timeout }
      worker.run()

      process.on 'SIGINT', =>
        console.log 'SIGINT caught, exiting'
        worker.stop (error) =>
          return @panic error if error?
          process.exit 0

      process.on 'SIGTERM', =>
        console.log 'SIGTERM caught, exiting'
        worker.stop (error) =>
          return @panic error if error?
          process.exit 0

  die: (error) =>
    return process.exit(0) unless error?
    console.error 'ERROR'
    console.error error.stack
    process.exit 1

module.exports = Command
