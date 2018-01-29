/*******************************************************************************
 * Copyright (c) 2013 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    http://www.eclipse.org/legal/epl-v10.html
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Andrew Banks - initial API and implementation and initial documentation
 *******************************************************************************/
/* @flow */
import ClientImplementation from './ClientImplementation';
import Message from './Message';
import { format, validate } from './util';
import { DEFAULT_KEEPALIVE_SECONDS, ERROR } from './constants';
import EventEmitter from 'events';

// ------------------------------------------------------------------------
// Public API.
// ------------------------------------------------------------------------

type ConstructorOptions = {
  uri: string,
  clientId: string,
  storage: any,
  webSocket?: Class<WebSocket>
}

type ConnectOptions = {
  userName?: string,
  password?: string,
  willMessage?: Message,
  timeout?: number,
  keepAliveInterval: number,
  useSSL: boolean,
  cleanSession: boolean,
  mqttVersion: number,
  allowMqttVersionFallback: boolean,
  uris?: string[]
}

/**
 * The JavaScript application communicates to the server using a {@link Client} object.
 *
 * Most applications will create just one Client object and then call its connect() method,
 * however applications can create more than one Client object if they wish.
 * In this case the combination of uri and clientId attributes must be different for each Client object.
 *
 * @name Client
 *
 * @fires Client#connectionLost
 * @fires Client#messageReceived
 * @fires Client#messageDelivered
 */
export default class Client extends EventEmitter {


  _client: ClientImplementation;

  /**
   *
   * @param {string} [uri] - the address of the messaging server, as a fully qualified WebSocket URI
   * @param {string} [clientId] - the Messaging client identifier, between 1 and 23 characters in length.
   * @param {object} [storage] - object implementing getItem, setItem, removeItem in a manner compatible with localStorage
   * @param {object} [webSocket] - object implementing the W3C websocket spec
   */
  constructor({ uri, clientId, storage, webSocket }: ConstructorOptions) {
    super();

    if (!/^(wss?):\/\/((\[(.+)\])|([^\/]+?))(:(\d+))?(\/.*)$/.test(uri)) {
      throw new Error(format(ERROR.INVALID_ARGUMENT, [typeof uri, 'uri']));
    }

    let clientIdLength = 0;
    for (let i = 0; i < clientId.length; i++) {
      let charCode = clientId.charCodeAt(i);
      if (charCode >= 0xD800 && charCode <= 0xDBFF) {
        i++; // Surrogate pair.
      }
      clientIdLength++;
    }
    if (typeof clientId !== 'string' || clientIdLength > 65535) {
      throw new Error(format(ERROR.INVALID_ARGUMENT, [clientId, 'clientId']));
    }

    this._client = new ClientImplementation(uri, clientId, storage, webSocket);

    /**
     * @event Client#messageDelivered
     * @type {Message}
     */
    this._client.onMessageDelivered = (message) => this.emit('messageDelivered', message);

    /**
     * @event Client#messageReceived
     * @type {Message}
     */
    this._client.onMessageArrived = (message) => this.emit('messageReceived', message);

    /**
     * @event Client#connectionLost
     * @type {Error}
     */
    this._client.onConnectionLost = (e) => this.emit('connectionLost', e);
  }

  /**
   * Connect this Messaging client to its server.
   *
   * @name Client#connect
   * @function
   * @param {number} [timeout=30000] - Fail if not connected within this time
   * @param {string} [userName] - Authentication username for this connection.
   * @param {string} [password] - Authentication password for this connection.
   * @param {Message} [willMessage] - sent by the server when the client disconnects abnormally.
   * @param {number} [keepAliveInterval=60] - ping the server every n seconds to avoid being disconnected by the remote end.
   * @param {number} [mqttVersion=4] - protocol version to use (3 or 4).
   * @param {boolean} [cleanSession=true] - if true the client and server persistent state is deleted on successful connect.
   */
  connect({
    userName,
    password,
    willMessage,
    timeout = 30000,
    keepAliveInterval = DEFAULT_KEEPALIVE_SECONDS,
    cleanSession = true,
    mqttVersion = 4
  }: ConnectOptions = {}) {
    validate({
      userName,
      password,
      willMessage,
      timeout,
      keepAliveInterval,
      cleanSession,
      mqttVersion
    }, {
      timeout: 'number',
      userName: '?string',
      password: '?string',
      willMessage: '?object',
      keepAliveInterval: 'number',
      cleanSession: 'boolean',
      mqttVersion: 'number'
    });

    return new Promise((resolve, reject) => {

      if (mqttVersion > 4 || mqttVersion < 3) {
        throw new Error(format(ERROR.INVALID_ARGUMENT, [mqttVersion, 'mqttVersion']));
      }

      //Check that if password is set, so is username
      if (password !== undefined && userName === undefined) {
        throw new Error(format(ERROR.INVALID_ARGUMENT, [password, 'password']));
      }

      if (willMessage) {
        if (!(willMessage instanceof Message)) {
          throw new Error(format(ERROR.INVALID_TYPE, [willMessage, 'willMessage']));
        }
        // The will message must have a payload that can be represented as a string.
        // Cause the willMessage to throw an exception if this is not the case.
        willMessage.payloadString;

        if (typeof willMessage.destinationName === 'undefined') {
          throw new Error(format(ERROR.INVALID_TYPE, [typeof willMessage.destinationName, 'willMessage.destinationName']));
        }
      }

      this._client.connect({
        userName,
        password,
        willMessage: willMessage || null,
        timeout,
        keepAliveInterval,
        cleanSession,
        mqttVersion: mqttVersion === 4 ? 4 : 3,
        onSuccess: resolve,
        onFailure: reject
      });
    });
  }

  /**
   * Subscribe for messages, request receipt of a copy of messages sent to the destinations described by the filter.
   *
   * @param {string} [filter] the topic to subscribe to
   * @param {number} [qos=0] - the maximum qos of any publications sent as a result of making this subscription.
   * @param {number} [timeout=30000] - milliseconds after which the call will fail
   * @returns {Promise}
   */
  subscribe(filter: string, { qos = 0, timeout = 30000 }:{ qos: 0 | 1 | 2, timeout: number } = {}) {
    return new Promise((resolve, reject) => {
      if (typeof filter !== 'string') {
        throw new Error('Invalid argument:' + filter);
      }
      if (typeof timeout !== 'number') {
        throw new Error('Invalid argument:' + timeout);
      }
      if ([0, 1, 2].indexOf(qos) === -1) {
        throw new Error('Invalid argument:' + qos);
      }

      this._client.subscribe(filter, {
        timeout,
        qos,
        onSuccess: resolve,
        onFailure: reject
      });
    });
  }

  /**
   * Unsubscribe for messages, stop receiving messages sent to destinations described by the filter.
   *
   * @param {string} [filter] the topic to unsubscribe from
   * @param {number} [timeout=30000] MS after which the promise will be rejected
   * @returns {Promise}
   */
  unsubscribe(filter: string, { timeout = 30000 }:{ timeout: number } = {}) {
    return new Promise((resolve, reject) => {
      if (typeof filter !== 'string') {
        throw new Error('Invalid argument:' + filter);
      }
      if (typeof timeout !== 'number') {
        throw new Error('Invalid argument:' + timeout);
      }

      this._client.unsubscribe(filter, {
        timeout,
        onSuccess: resolve,
        onFailure: reject
      });
    });
  }

  /**
   * Send a message to the consumers of the destination in the Message.
   *
   * @name Client#send
   * @function
   * @param {string|Message} topic - <b>mandatory</b> The name of the destination to which the message is to be sent.
   *             - If it is the only parameter, used as Message object.
   * @param {String|ArrayBuffer} payload - The message data to be sent.
   * @param {number} qos The Quality of Service used to deliver the message.
   *    <dl>
   *      <dt>0 Best effort (default).
   *          <dt>1 At least once.
   *          <dt>2 Exactly once.
   *    </dl>
   * @param {Boolean} retained If true, the message is to be retained by the server and delivered
   *                     to both current and future subscriptions.
   *                     If false the server only delivers the message to current subscribers, this is the default for new Messages.
   *                     A received message has the retained boolean set to true if the message was published
   *                     with the retained boolean set to true
   *                     and the subscrption was made after the message has been published.
   * @throws {InvalidState} if the client is not connected.
   */
  send(topic: string | Message, payload: string, qos: 0 | 1 | 2, retained: boolean) {
    let message;

    if (arguments.length === 0) {
      throw new Error('Invalid argument.' + 'length');

    } else if (arguments.length === 1) {

      if (!(topic instanceof Message)) {
        throw new Error('Invalid argument:' + typeof topic);
      }

      message = topic;
      if (typeof message.destinationName === 'undefined') {
        throw new Error(format(ERROR.INVALID_ARGUMENT, [message.destinationName, 'Message.destinationName']));
      }
      this._client.send(message);

    } else if (typeof topic === 'string') {
      //parameter checking in Message object
      message = new Message(payload);
      message.destinationName = topic;
      if (arguments.length >= 3) {
        message.qos = qos;
      }
      if (arguments.length >= 4) {
        message.retained = retained;
      }
      this._client.send(message);
    }
  }

  /**
   * Normal disconnect of this Messaging client from its server.
   *
   * @name Client#disconnect
   * @function
   * @throws {InvalidState} if the client is already disconnected.
   */
  disconnect() {
    return new Promise((resolve, reject) => {
      this.once('connectionLost', (error) => {
        if (error && error.errorCode !== 0) {
          return reject(error);
        }
        resolve();
      });
      this._client.disconnect();
    });
  }

  /**
   * Get the contents of the trace log.
   *
   * @name Client#getTraceLog
   * @function
   * @return {Object[]} tracebuffer containing the time ordered trace records.
   */
  getTraceLog() {
    return this._client.getTraceLog();
  }

  /**
   * Start tracing.
   *
   * @name Client#startTrace
   * @function
   */
  startTrace() {
    this._client.startTrace();
  }

  /**
   * Stop tracing.
   *
   * @name Client#stopTrace
   * @function
   */
  stopTrace() {
    this._client.stopTrace();
  }

  isConnected() {
    return this._client.connected;
  }

  get uri(): string {
    return this._client.uri;
  }

  get clientId(): ?string {
    return this._client.clientId;
  }

  get trace(): ?Function {
    return this._client.traceFunction;
  }

  set trace(trace: Function) {
    if (typeof trace === 'function') {
      this._client.traceFunction = trace;
    } else {
      throw new Error(format(ERROR.INVALID_TYPE, [typeof trace, 'onTrace']));
    }
  }
}
