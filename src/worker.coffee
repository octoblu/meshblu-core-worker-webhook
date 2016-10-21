class Worker
  constructor: (options={})->
    { @redis, @queueName, @queueTimeout } = options
    throw new Error('Worker: requires redis') unless @redis?
    throw new Error('Worker: requires queueName') unless @queueName?
    throw new Error('Worker: requires queueTimeout') unless @queueTimeout?
    @shouldStop = false
    @isStopped = false

  do: (callback) =>
    @redis.brpop @queueName, @queueTimeout, (error, result) =>
      return callback error if error?
      return callback() unless result?

      [ queue, data ] = result
      try
        data = JSON.parse data
      catch error
        return callback error

      @_process data, (error) =>
        console.log error.stack if error?
        callback()

    return # avoid returning promise

  run: =>
    async.doUntil @do, (=> @shouldStop), =>
      @isStopped = true

  stop: (callback) =>
    @shouldStop = true

    timeout = setTimeout =>
      clearInterval interval
      callback new Error 'Stop Timeout Expired'
    , 5000

    interval = setInterval =>
      return unless @isStopped?
      clearInterval interval
      clearTimeout timeout
      callback()
    , 250

  _process: (data, callback) =>
    callback null

module.exports = Worker
