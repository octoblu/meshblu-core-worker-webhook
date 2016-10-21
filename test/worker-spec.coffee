Worker  = require '../src/worker'
Redis   = require 'ioredis'
RedisNS = require '@octoblu/redis-ns'

describe 'Worker', ->
  beforeEach (done) ->
    client = new Redis 'localhost', dropBufferSupport: true
    client.on 'ready', =>
      @redis = new RedisNS 'test-worker', client
      @redis.del 'work', done

  beforeEach ->
    queueName = 'work'
    queueTimeout = 1
    @sut = new Worker { @redis, queueName, queueTimeout }

  afterEach (done) ->
    @sut.stop done

  describe '->do', ->
    context 'test', ->
      beforeEach (done) ->
        data =
          test: 'test-it'

        record = JSON.stringify data
        @redis.lpush 'work', record, done
        return # stupid promises

      beforeEach (done) ->
        @sut.do done

      it 'it should not blow up', ->
        expect(true).to.be.true
