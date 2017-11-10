Worker         = require '../src/worker'
{ privateKey } = require './some-private-key.json'

describe 'Worker->validateURL', ->
  beforeEach ->
    @redisUri = 'localhost'
    @namespace = 'test-job-logger'
    queueName = 'work'
    queueTimeout = 1
    meshbluConfig =
      hostname: 'localhost'
      port: 0xbabe
      protocol: 'http'

    @consoleError = sinon.spy()
    @sut = new Worker {
      client: true,
      jobLogSampleRate: '1.00',
      workLogger: true,
      jobLogger: true,
      requestTimeout: 1,
      queueName,
      queueTimeout,
      privateKey,
      meshbluConfig,
      @consoleError
      @namespace
      @redisUri
    }

  describe 'when the domain is the just a domain', ->
    beforeEach (done) ->
      @sut.validateURL {
        url: 'https://nanocyte-engine/something'
      }, (@error) =>
        done()

    it 'should be ok', ->
      expect(@error).not.to.exist
      
  describe 'when the domain is the string undefined', ->
    beforeEach (done) ->
      @sut.validateURL {
        url: 'https://exchange-sync.undefined/something'
      }, (@error) =>
        done()

    it 'should yield a 422 error', ->
      expect(@error.code).to.equal 422

  describe 'when the domain is the string null', ->
    beforeEach (done) ->
      @sut.validateURL {
        url: 'https://exchange-sync.null/something'
      }, (@error) =>
        done()

    it 'should yield a 422 error', ->
      expect(@error.code).to.equal 422

  describe 'when no options are passed', ->
    beforeEach (done) ->
      @sut.validateURL {}, (@error) =>
        done()

    it 'should yield a 422 error', ->
      expect(@error.code).to.equal 422

  describe 'when no valid options are passed', ->
    beforeEach (done) ->
      @sut.validateURL {
        url: {}
        baseUrl: []
        uri: true
      }, (@error) =>
        done()

    it 'should yield a 422 error', ->
      expect(@error.code).to.equal 422

  describe 'when url is not a string', ->
    beforeEach (done) ->
      @sut.validateURL {
        url: {}
      }, (@error) =>
        done()

    it 'should yield a 422 error', ->
      expect(@error.code).to.equal 422

  describe 'when url is not a string but baseUrl and uri are', ->
    beforeEach (done) ->
      @sut.validateURL {
        url: {}
        baseUrl: 'https://test.octoblu.com'
        uri: 'some/path'
      }, (@error) =>
        done()

    it 'should not have an error', ->
      expect(@error).to.not.exist

  describe 'when url is not a string', ->
    beforeEach (done) ->
      @sut.validateURL {
        url: {}
      }, (@error) =>
        done()

    it 'should yield a 422 error', ->
      expect(@error.code).to.equal 422

  describe 'when using the baseUrl and uri', ->
    beforeEach (done) ->
      @sut.validateURL {
        baseUrl: 'https://test.octoblu.com'
        uri: 'some/path'
      }, (@error) =>
        done()

    it 'should not have an error', ->
      expect(@error).to.not.exist

  describe 'when using the baseUrl and uri with a prefixed slash', ->
    beforeEach (done) ->
      @sut.validateURL {
        baseUrl: 'https://test.octoblu.com'
        uri: '/some/path'
      }, (@error) =>
        done()

    it 'should not have an error', ->
      expect(@error).to.not.exist

  describe 'when using a correct url', ->
    beforeEach (done) ->
      @sut.validateURL {
        url: 'https://test.octoblu.com/some/path'
      }, (@error) =>
        done()

    it 'should not have an error', ->
      expect(@error).to.not.exist

  describe 'when using a nanocyte-engine url', ->
    beforeEach (done) ->
      @sut.validateURL {
        url: 'https://nanocyte-engine.octoblu.com/flows/123/instances/456/messages'
      }, (@error) =>
        done()

    it 'should not have an error', ->
      expect(@error).to.not.exist

  describe 'when using a endo-meshblu url', ->
    beforeEach (done) ->
      @sut.validateURL {
        url: 'https://endo-meshblu.octoblu.com/v2/messages'
      }, (@error) =>
        done()

    it 'should not have an error', ->
      expect(@error).to.not.exist

  describe 'when using a credentials url', ->
    beforeEach (done) ->
      @sut.validateURL {
        url: 'https://credentials.octoblu.com/request'
      }, (@error) =>
        done()

    it 'should not have an error', ->
      expect(@error).to.not.exist

  describe 'when using an invalid url', ->
    beforeEach (done) ->
      @sut.validateURL {
        url: 'https://test.octoblu./some/path'
      }, (@error) =>
        done()

    it 'should yield a 422 error', ->
      expect(@error.code).to.equal 422

  describe 'when missing the protocol', ->
    beforeEach (done) ->
      @sut.validateURL {
        url: '//test.octoblu.com/some/path'
      }, (@error) =>
        done()

    it 'should yield a 422 error', ->
      expect(@error.code).to.equal 422

  describe 'when only using a path', ->
    beforeEach (done) ->
      @sut.validateURL {
        url: '/some/path'
      }, (@error) =>
        done()

    it 'should yield a 422 error', ->
      expect(@error.code).to.equal 422
