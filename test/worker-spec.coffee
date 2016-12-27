Worker         = require '../src/worker'
shmock         = require 'shmock'
enableDestroy  = require 'server-destroy'
Redis          = require 'ioredis'
JobLogger      = require 'job-logger'
{ privateKey } = require './some-private-key.json'
RedisNS        = require '@octoblu/redis-ns'

describe 'Worker->doAndDrain', ->
  beforeEach (done) ->
    redisClient = new Redis 'localhost', dropBufferSupport: true
    redisClient.once 'ready', =>
      @client = new RedisNS 'test-worker', redisClient
      @client.del 'work', done

  beforeEach (done) ->
    redisClient = new Redis 'localhost', dropBufferSupport: true
    redisClient.once 'ready', =>
      client = new RedisNS 'test-job-logger', redisClient
      @jobLogger = new JobLogger {
        client,
        indexPrefix: 'test:metric:webhook',
        type: 'meshblu-core-worker-webhook:job'
        jobLogQueue: 'sample-rate:1.00'
      }
      done()

  beforeEach (done) ->
    redisClient = new Redis 'localhost', dropBufferSupport: true
    redisClient.once 'ready', =>
      client = new RedisNS 'test-job-logger', redisClient
      @workLogger = new JobLogger {
        client,
        indexPrefix: 'test:metric:webhook',
        type: 'meshblu-core-worker-webhook:work'
        jobLogQueue: 'sample-rate:1.00'
      }
      done()

  beforeEach ->
    @dumbServer = shmock 0xd00d
    enableDestroy @dumbServer
    @dumbBaseUrl = "http://localhost:#{0xd00d}"
    @meshbluServer = shmock 0xbabe
    enableDestroy @meshbluServer

  beforeEach ->
    queueName = 'work'
    queueTimeout = 1
    meshbluConfig =
      hostname: 'localhost'
      port: 0xbabe
      protocol: 'http'

    @consoleError = sinon.spy()
    @sut = new Worker {
      @client,
      jobLogSampleRate: '1.00',
      @workLogger,
      @jobLogger,
      requestTimeout: 1,
      queueName,
      queueTimeout,
      privateKey,
      meshbluConfig,
      @consoleError
    }

  afterEach ->
    @dumbServer.destroy()
    @meshbluServer.destroy()

  describe 'when webhook is invalid', ->
    beforeEach (done) ->
      data =
        revokeOptions:
          uuid: 'dumb-uuid'
          token: 'dumb-token'
        requestOptions:
          method: 'POST'
          url: 'https://exchange-sync.undefined/something'
          json:
            some: 'data'

      record = JSON.stringify data
      @client.lpush 'work', record, done
      return # stupid promises

    beforeEach (done) ->
      dumbAuth = new Buffer('dumb-uuid:dumb-token').toString('base64')

      @revokeToken = @meshbluServer
        .delete '/devices/dumb-uuid/tokens/dumb-token'
        .set 'Authorization', "Basic #{dumbAuth}"
        .reply 204

      @sut.doAndDrain done

    it 'should expire the token', ->
      @revokeToken.done()

    it 'should call consoleError with the correct key', ->
      expect(@consoleError).to.have.been.calledWith 'Process Error'

    it 'should call consoleError with the correct error', ->
      expect(@consoleError.args[0][1].code).to.equal 422

  describe 'POST /dumb/hook', ->
    beforeEach (done) ->
      data =
        revokeOptions:
          uuid: 'dumb-uuid'
          token: 'dumb-token'
        requestOptions:
          method: 'POST'
          uri: '/dumb/hook'
          baseUrl: @dumbBaseUrl
          json:
            some: 'data'
            no:   'data'
            who:  'knows'

      record = JSON.stringify data
      @client.lpush 'work', record, done
      return # stupid promises

    describe 'when both requests are succesful', ->
      beforeEach (done) ->
        dumbAuth = new Buffer('dumb-uuid:dumb-token').toString('base64')

        @revokeToken = @meshbluServer
          .delete '/devices/dumb-uuid/tokens/dumb-token'
          .set 'Authorization', "Basic #{dumbAuth}"
          .reply 204

        @dumbHook = @dumbServer
          .post '/dumb/hook'
          .send {
            some: 'data'
            no: 'data'
            who: 'knows'
          }
          .reply 204

        @sut.doAndDrain done

      it 'should hit up the webhook', ->
        @dumbHook.done()

      it 'should expire the token', ->
        @revokeToken.done()

    describe 'when the webhook request times out', ->
      beforeEach (done) ->
        dumbAuth = new Buffer('dumb-uuid:dumb-token').toString('base64')

        @revokeToken = @meshbluServer
          .delete '/devices/dumb-uuid/tokens/dumb-token'
          .set 'Authorization', "Basic #{dumbAuth}"
          .reply 204

        @dumbHook = @dumbServer
          .post '/dumb/hook'
          .send {
            some: 'data'
            no: 'data'
            who: 'knows'
          }
          .delay 1100
          .reply 204

        @sut.doAndDrain done

      it 'should expire the token', ->
        @revokeToken.done()

    describe 'when the revokeToken request times out', ->
      beforeEach (done) ->
        dumbAuth = new Buffer('dumb-uuid:dumb-token').toString('base64')

        @revokeToken = @meshbluServer
          .delete '/devices/dumb-uuid/tokens/dumb-token'
          .set 'Authorization', "Basic #{dumbAuth}"
          .delay 1100
          .reply 204

        @dumbHook = @dumbServer
          .post '/dumb/hook'
          .send {
            some: 'data'
            no: 'data'
            who: 'knows'
          }
          .reply 204

        @sut.doAndDrain done

      it 'should hitting up the webhook', ->
        @dumbHook.done()

    describe 'when the webhook request fails', ->
      beforeEach (done) ->
        dumbAuth = new Buffer('dumb-uuid:dumb-token').toString('base64')

        @revokeToken = @meshbluServer
          .delete '/devices/dumb-uuid/tokens/dumb-token'
          .set 'Authorization', "Basic #{dumbAuth}"
          .reply 204

        @dumbHook = @dumbServer
          .post '/dumb/hook'
          .send {
            some: 'data'
            no: 'data'
            who: 'knows'
          }
          .reply 500

        @sut.doAndDrain done

      it 'should hit up the webhook', ->
        @dumbHook.done()

      it 'should expire the token', ->
        @revokeToken.done()

      it 'should not log the error', ->
        expect(@consoleError).to.not.have.been.called

    describe 'when the revoke request fails', ->
      beforeEach (done) ->
        dumbAuth = new Buffer('dumb-uuid:dumb-token').toString('base64')

        @revokeToken = @meshbluServer
          .delete '/devices/dumb-uuid/tokens/dumb-token'
          .set 'Authorization', "Basic #{dumbAuth}"
          .reply 500

        @dumbHook = @dumbServer
          .post '/dumb/hook'
          .send {
            some: 'data'
            no: 'data'
            who: 'knows'
          }
          .reply 200

        @sut.doAndDrain done

      it 'should hit up the webhook', ->
        @dumbHook.done()

      it 'should expire the token', ->
        @revokeToken.done()

      it 'should log the error', ->
        expect(@consoleError).to.have.been.called

  describe 'POST /dumb/hook/signed', ->
    beforeEach (done) ->
      data =
        signRequest: true
        requestOptions:
          method: 'POST'
          uri: '/dumb/hook/signed'
          baseUrl: @dumbBaseUrl
          headers:
            'X-MESHBLU-UUID': 'dumb-uuid'
          json:
            some: 'data'
            no:   'data'
            who:  'knows'

      record = JSON.stringify data
      @client.lpush 'work', record, done
      return # stupid promises

    describe 'when it hits up the webhook', ->
      beforeEach (done) ->
        dumbAuth = new Buffer('dumb-uuid:dumb-token').toString('base64')

        @revokeToken = @meshbluServer
          .delete '/devices/dumb-uuid/tokens/dumb-token'
          .set 'Authorization', "Basic #{dumbAuth}"
          .reply 204

        @dumbHook = @dumbServer
          .post '/dumb/hook/signed'
          .set 'X-MESHBLU-UUID', 'dumb-uuid'
          .send {
            some: 'data'
            no: 'data'
            who: 'knows'
          }
          .reply 204

        @sut.doAndDrain done

      it 'should hit up the webhook', ->
        @dumbHook.done()

      it 'should not expire the token', ->
        expect(@revokeToken.isDone).to.be.false

  describe 'POST /dumb/hook/no-auth', ->
    beforeEach (done) ->
      data =
        requestOptions:
          method: 'POST'
          uri: '/dumb/hook/no-auth'
          baseUrl: @dumbBaseUrl
          json:
            some: 'data'
            no:   'data'
            who:  'knows'

      record = JSON.stringify data
      @client.lpush 'work', record, done
      return # stupid promises

    describe 'when it hits up the webhook', ->
      beforeEach (done) ->
        dumbAuth = new Buffer('dumb-uuid:dumb-token').toString('base64')

        @revokeToken = @meshbluServer
          .delete '/devices/dumb-uuid/tokens/dumb-token'
          .set 'Authorization', "Basic #{dumbAuth}"
          .reply 204

        @dumbHook = @dumbServer
          .post '/dumb/hook/no-auth'
          .send {
            some: 'data'
            no: 'data'
            who: 'knows'
          }
          .reply 204

        @sut.doAndDrain done

      it 'should hit up the webhook', ->
        @dumbHook.done()

      it 'should not expire the token', ->
        expect(@revokeToken.isDone).to.be.false
