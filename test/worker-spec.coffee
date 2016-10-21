Worker         = require '../src/worker'
shmock         = require 'shmock'
enableDestroy  = require 'server-destroy'
Redis          = require 'ioredis'
{ privateKey } = require './some-private-key.json'
RedisNS        = require '@octoblu/redis-ns'

describe 'Worker', ->
  beforeEach (done) ->
    client = new Redis 'localhost', dropBufferSupport: true
    client.on 'ready', =>
      @redis = new RedisNS 'test-worker', client
      @redis.del 'work', done

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

    @logFn = sinon.spy()
    @sut = new Worker { @redis, queueName, queueTimeout, privateKey, meshbluConfig, @logFn }

  afterEach (done) ->
    @sut.stop done

  afterEach ->
    @dumbServer.destroy()
    @meshbluServer.destroy()

  describe '->do', ->
    context 'POST /dumb/hook', ->
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
        @redis.lpush 'work', record, done
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

          @sut.do done

        it 'should hit up the webhook', ->
          @dumbHook.done()

        it 'should expire the token', ->
          @revokeToken.done()

      describe 'when the main request fails', ->
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

          @sut.do done

        it 'should hit up the webhook', ->
          @dumbHook.done()

        it 'should expire the token', ->
          @revokeToken.done()

        it 'should not log the error', ->
          expect(@logFn).to.not.have.been.called

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
            .reply 500

          @sut.do done

        it 'should hit up the webhook', ->
          @dumbHook.done()

        it 'should expire the token', ->
          @revokeToken.done()

        it 'should log the error', ->
          expect(@logFn).to.have.been.called

    context 'POST /dumb/hook/signed', ->
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
        @redis.lpush 'work', record, done
        return # stupid promises

      describe 'when both requests are succesful', ->
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

          @sut.do done

        it 'should hit up the webhook', ->
          @dumbHook.done()

        it 'should not expire the token', ->
          expect(@revokeToken.isDone).to.be.false
