var amqp = require('amqplib'),
  Promise = require('es6-promise').Promise,
  uuid = require('uuid');

exports.connect = function (url, serviceName) {
  return amqp.connect(url)
    .then(function (connection) {
      return new SimpleServiceQueueConnection(connection, serviceName);
    });
};

function SimpleServiceQueueConnection(connection, serviceName) {
  this.connection = connection;
  this.serviceName = serviceName;
}

SimpleServiceQueueConnection.prototype.emit = function (eventName, payload) {
  this._getSenderChannelPromise(eventName, 'fanout')
    .then(function (channel) {
      var content = convertJSONToBuffer(payload);
      channel.publish(eventName, '', content);
    });
};

SimpleServiceQueueConnection.prototype.listenTo = function (eventName, callback) {
  var self = this;
  this._getChannelPromiseForEventName(eventName, 'fanout')
    .then(function (channel) {
      var queueName = self._getQueueNameForServiceAndEvent(self.serviceName, eventName);
      channel.consume(queueName, function (msg) {
        var payload = convertBufferToJSON(msg.content);
        callback(payload);
        channel.ack(msg);
      })
    });
};

SimpleServiceQueueConnection.prototype.call = function (procedureName, payload) {
  return this._getSenderChannelPromise(procedureName, 'direct')
    .then(function (channel) {
      return channel.assertQueue(null, {exclusive: true, autoDelete: true})
        .then(function (queue) {
          var replyToQueue = queue.queue;
          var correlationId = uuid();

          var promise = new Promise(function (resolve) {
            channel.consume(replyToQueue, function (msg) {
              if (msg.properties.correlationId === correlationId) {
                var responsePayload = convertBufferToJSON(msg.content);
                resolve(responsePayload);
              }
            });
          });

          var options = {
            correlationId: correlationId,
            replyTo: replyToQueue
          };
          var content = convertJSONToBuffer(payload);
          channel.publish(procedureName, '', content, options);

          return promise;
        });
    })
};

SimpleServiceQueueConnection.prototype.respondTo = function (procedureName, callback) {
  var self = this;
  this._getChannelPromiseForEventName(procedureName, 'direct')
    .then(function (channel) {
      var queueName = self._getQueueNameForServiceAndEvent(self.serviceName, procedureName);
      channel.consume(queueName, function (msg) {
        var requestPayload = convertBufferToJSON(msg.content);
        callback(requestPayload, responseCallback);

        function responseCallback(responseBody) {
          var responseQueue = msg.properties.replyTo;
          var options = {
            correlationId: msg.properties.correlationId
          };
          var payload = convertJSONToBuffer(responseBody);
          channel.sendToQueue(responseQueue, payload, options);
          channel.ack(msg);
        }
      });
    });
};

SimpleServiceQueueConnection.prototype._getChannelPromiseForEventName = function (eventName, exchangeType) {
  var self = this;
  return this.connection.createChannel()
    .then(function (channel) {
      var queueName = self._getQueueNameForServiceAndEvent(self.serviceName, eventName);
      channel.assertQueue(queueName);
      channel.assertExchange(eventName, exchangeType);
      channel.bindQueue(queueName, eventName, '');
      return channel;
    });
};

SimpleServiceQueueConnection.prototype._getSenderChannelPromise = function (eventName, eventType) {
  return this.connection.createChannel()
    .then(function (channel) {
      channel.assertExchange(eventName, eventType);
      return channel;
    });
};

SimpleServiceQueueConnection.prototype._getQueueNameForServiceAndEvent = function (service, eventName) {
  return service + ':' + eventName;
};

function convertJSONToBuffer(jsonObject) {
  var jsonString = JSON.stringify(jsonObject);
  return new Buffer(jsonString);
}

function convertBufferToJSON(buffer) {
  var jsonString = buffer.toString();
  return JSON.parse(jsonString);
}