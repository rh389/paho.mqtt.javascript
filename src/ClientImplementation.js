/** @flow */

import Message from './Message';
import { decodeMessage, format, invariant } from './util';
import { CONNACK_RC, ERROR, MESSAGE_TYPE } from './constants';
import Pinger from './Pinger';
import WireMessage from './WireMessage';
import PublishMessage from './PublishMessage';
import ConnectMessage from './ConnectMessage';

type ConnectOptions = {
  timeout?: number,
  mqttVersion: 3 | 4,
  keepAliveInterval: number,
  onSuccess: ?() => void,
  onFailure: ?(Error) => void,
  userName?: string,
  password?: string,
  willMessage: ?Message,
  cleanSession: boolean
}

type Storage = Object & { setItem: (key: string, item: any) => void, getItem: (key: string) => any };

/*
 * Internal implementation of the Websockets MQTT V3.1/V4 client.
 */
class ClientImplementation {

  uri: string;
  clientId: string;
  storage: Storage;
  webSocket: Class<WebSocket>;
  socket: ?WebSocket;

  // We have received a CONNACK and not subsequently disconnected
  connected = false;

  // The largest permitted message ID, effectively number of in-flight outbound messages.
  // Must be <=65536, the maximum permitted by the protocol
  maxOutboundInFlight = 65536;
  connectOptions: ?ConnectOptions;
  onConnectionLost: ?Function;
  onMessageDelivered: ?Function;
  onMessageArrived: ?Function;
  traceFunction: ?Function;
  _msg_queue = null;
  _connectTimeout: ?number;
  /* The sendPinger monitors how long we allow before we send data to prove to the server that we are alive. */
  sendPinger = null;

  receiveBuffer: ?Uint8Array = null;

  _traceBuffer = null;
  _MAX_TRACE_ENTRIES = 100;

  // Internal queue of messages to be sent, in sending order.
  _messagesAwaitingDispatch: (WireMessage | PublishMessage)[] = [];

  // Messages we have sent and are expecting a response for, indexed by their respective message ids.
  _outboundMessagesInFlight: { [key: string]: WireMessage | PublishMessage } = {};

  // Messages we have received and acknowleged and are expecting a confirm message for indexed by their respective message ids.
  _receivedMessagesAwaitingAckConfirm: { [key: string]: WireMessage | PublishMessage } = {};

  // Unique identifier for outbound messages, incrementing counter as messages are sent.
  _nextMessageId: number = 1;

  // Used to determine the transmission sequence of stored sent messages.
  _sequence: number = 0;

  // Local storage keys are qualified with this.
  _localKey: string;

  /**
   *
   * @param {string} [uri] the FQDN (with protocol and path suffix) of the host
   * @param {string} [clientId] the MQ client identifier.
   * @param {object} [storage] object implementing getItem, setItem, removeItem in a manner compatible with localStorage
   * @param {object} [ws] object implementing the W3C websockets spec
   */
  constructor(uri: string, clientId: string, storage?: Object, ws?: Class<WebSocket>) {
    // Check dependencies are satisfied in this browser.
    if (!ws && !(window && window.WebSocket)) {
      throw new Error(format(ERROR.UNSUPPORTED, ['WebSocket']));
    }
    if (!storage && !(window && window.localStorage)) {
      throw new Error(format(ERROR.UNSUPPORTED, ['localStorage']));
    }

    if (!(window && window.ArrayBuffer)) {
      throw new Error(format(ERROR.UNSUPPORTED, ['ArrayBuffer']));
    }
    this._trace('Client', uri, clientId);

    this.uri = uri;
    this.clientId = clientId;
    this.storage = storage || window.localStorage;
    this.webSocket = ws || window.WebSocket;

    // Local storagekeys are qualified with the following string.
    // The conditional inclusion of path in the key is for backward
    // compatibility to when the path was not configurable and assumed to
    // be /mqtt
    this._localKey = uri + ':' + clientId + ':';


    // Load the local state, if any, from the saved version, only restore state relevant to this client.
    Object.keys(this.storage).forEach(key => {
      if (key.indexOf('Sent:' + this._localKey) === 0 || key.indexOf('Received:' + this._localKey) === 0) {
        this.restore(key);
      }
    });
  }


  connect(connectOptions: ConnectOptions) {
    const maskedCopy = { ...connectOptions };
    if (maskedCopy.password) {
      maskedCopy.password = 'REDACTED';
    }
    this._trace('Client.connect', maskedCopy, this.socket, this.connected);

    if (this.connected) {
      throw new Error(format(ERROR.INVALID_STATE, ['already connected']));
    }
    if (this.socket) {
      throw new Error(format(ERROR.INVALID_STATE, ['already connected']));
    }

    this.connectOptions = connectOptions;
    this.connected = false;
    this.socket = new this.webSocket(this.uri, ['mqtt' + (connectOptions.mqttVersion === 3 ? 'v3.1' : '')]);
    this.socket.binaryType = 'arraybuffer';

    // When the socket is open, this client will send the CONNECT WireMessage using the saved parameters.
    this.socket.onopen = () => {
      const { willMessage, mqttVersion, userName, password, cleanSession, keepAliveInterval } = connectOptions;
      // Send the CONNECT message object.

      const wireMessage = new ConnectMessage({
        cleanSession,
        userName,
        password,
        mqttVersion,
        willMessage,
        keepAliveInterval,
        clientId: this.clientId
      });

      this._trace('socket.send', { ...wireMessage, options: maskedCopy });

      this.socket && (this.socket.onopen = () => null);
      this.socket && (this.socket.send(wireMessage.encode()));
    };

    this.socket.onmessage = (event) => {
      this._trace('socket.onmessage', event.data);
      const messages = this._deframeMessages(((event.data:any):ArrayBuffer));
      messages && messages.forEach(message => this._handleMessage(message));
    };
    this.socket.onerror = (error: { data?: string }) =>
      this._disconnected(ERROR.SOCKET_ERROR.code, format(ERROR.SOCKET_ERROR, [error.data || ' Unknown socket error']));
    this.socket.onclose = () => this._disconnected(ERROR.SOCKET_CLOSE.code, format(ERROR.SOCKET_CLOSE));

    if (connectOptions.keepAliveInterval > 0) {
      //Cast this to any to deal with flow/IDE bug: https://github.com/facebook/flow/issues/2235#issuecomment-239357626
      this.sendPinger = new Pinger((this: any), connectOptions.keepAliveInterval);
    }

    if (connectOptions.timeout) {
      this._connectTimeout = setTimeout(() => {
        this._disconnected(ERROR.CONNECT_TIMEOUT.code, format(ERROR.CONNECT_TIMEOUT));
      }, connectOptions.timeout);
    }
  }

  subscribe(filter: string, subscribeOptions: { onSuccess: ({ grantedQos: number }) => void, onFailure: (Error) => void, qos: 0 | 1 | 2, timeout: number }) {
    this._trace('Client.subscribe', filter, subscribeOptions);

    if (!this.connected) {
      throw new Error(format(ERROR.INVALID_STATE, ['not connected']));
    }

    const wireMessage = new WireMessage(MESSAGE_TYPE.SUBSCRIBE);
    wireMessage.topics = [filter];
    wireMessage.requestedQos = [subscribeOptions.qos || 0];

    if (subscribeOptions.onSuccess) {
      wireMessage.subAckReceived = function (grantedQos) {
        subscribeOptions.onSuccess({ grantedQos: grantedQos });
      };
    }

    if (subscribeOptions.onFailure) {
      wireMessage.onFailure = subscribeOptions.onFailure;
    }

    if (subscribeOptions.timeout && subscribeOptions.onFailure) {
      wireMessage.timeOut = setTimeout(() => {
        subscribeOptions.onFailure(new Error(format(ERROR.SUBSCRIBE_TIMEOUT)));
      }, subscribeOptions.timeout);
    }

    // All subscriptions return a SUBACK.
    this._requires_ack(wireMessage);
    this._scheduleMessage(wireMessage);
  }

  /** @ignore */
  unsubscribe(filter: string, unsubscribeOptions: { onSuccess: () => void, onFailure: (Error) => void, timeout: number }) {
    this._trace('Client.unsubscribe', filter, unsubscribeOptions);

    if (!this.connected) {
      throw new Error(format(ERROR.INVALID_STATE, ['not connected']));
    }

    const wireMessage = new WireMessage(MESSAGE_TYPE.UNSUBSCRIBE);
    wireMessage.topics = [filter];

    if (unsubscribeOptions.onSuccess) {
      wireMessage.unSubAckReceived = function () {
        unsubscribeOptions.onSuccess();
      };
    }
    if (unsubscribeOptions.timeout) {
      wireMessage.timeOut = setTimeout(() => {
        unsubscribeOptions.onFailure(new Error(format(ERROR.UNSUBSCRIBE_TIMEOUT)));
      }, unsubscribeOptions.timeout);
    }

    // All unsubscribes return a SUBACK.
    this._requires_ack(wireMessage);
    this._scheduleMessage(wireMessage);
  }

  send(message: Message) {
    this._trace('Client.send', message);

    if (!this.connected) {
      throw new Error(format(ERROR.INVALID_STATE, ['not connected']));
    }

    const wireMessage = new PublishMessage(message);

    if (message.qos > 0) {
      this._requires_ack(wireMessage);
    } else if (this.onMessageDelivered) {
      const onMessageDelivered = this.onMessageDelivered;
      wireMessage.onDispatched = () => onMessageDelivered(wireMessage.payloadMessage);
    }
    this._scheduleMessage(wireMessage);
  }

  disconnect() {
    this._trace('Client.disconnect');

    if (!this.socket) {
      throw new Error(format(ERROR.INVALID_STATE, ['not connecting or connected']));
    }

    const wireMessage = new WireMessage(MESSAGE_TYPE.DISCONNECT);

    // Run the disconnected call back as soon as the message has been sent,
    // in case of a failure later on in the disconnect processing.
    // as a consequence, the _disconnected call back may be run several times.
    wireMessage.onDispatched = () => {
      this._disconnected();
    };

    this._scheduleMessage(wireMessage);
  }

  getTraceLog() {
    if (this._traceBuffer !== null) {
      this._trace('Client.getTraceLog', new Date());
      this._trace('Client.getTraceLog in flight messages', Object.keys(this._outboundMessagesInFlight).length);
      Object.keys(this._outboundMessagesInFlight).forEach((key) => {
        this._trace('_outboundMessagesInFlight ', key, this._outboundMessagesInFlight[key]);
      });
      Object.keys(this._receivedMessagesAwaitingAckConfirm).forEach((key) => {
        this._trace('_receivedMessagesAwaitingAckConfirm ', key, this._receivedMessagesAwaitingAckConfirm[key]);
      });
      return this._traceBuffer;
    }
  }

  startTrace() {
    if (this._traceBuffer === null) {
      this._traceBuffer = [];
    }
    this._trace('Client.startTrace', new Date());
  }

  stopTrace() {
    this._traceBuffer = null;
  }


  // Schedule a new message to be sent over the WebSockets
  // connection. CONNECT messages cause WebSocket connection
  // to be started. All other messages are queued internally
  // until this has happened. When WS connection starts, process
  // all outstanding messages.
  _scheduleMessage(message: WireMessage | PublishMessage) {
    this._messagesAwaitingDispatch.push(message);
    // Process outstanding messages in the queue if we have an  open socket, and have received CONNACK.
    if (this.connected) {
      this._process_queue();
    }
  }

  store(prefix: string, wireMessage: PublishMessage) {
    const messageIdentifier = wireMessage.messageIdentifier;
    invariant(messageIdentifier, format(ERROR.INVALID_STATE, ['Cannot store a WireMessage with no messageIdentifier']));
    const storedMessage: any = { type: wireMessage.type, messageIdentifier, version: 1 };

    switch (wireMessage.type) {
      case MESSAGE_TYPE.PUBLISH:
        const payloadMessage = wireMessage.payloadMessage;
        invariant(payloadMessage, format(ERROR.INVALID_STATE, ['PUBLISH WireMessage with no payloadMessage']));
        if (wireMessage.pubRecReceived) {
          storedMessage.pubRecReceived = true;
        }

        // Convert the payload to a hex string.
        storedMessage.payloadMessage = {};
        let hex = '';
        const messageBytes = payloadMessage.payloadBytes;
        for (let i = 0; i < messageBytes.length; i++) {
          if (messageBytes[i] <= 0xF) {
            hex = hex + '0' + messageBytes[i].toString(16);
          } else {
            hex = hex + messageBytes[i].toString(16);
          }
        }
        storedMessage.payloadMessage.payloadHex = hex;

        storedMessage.payloadMessage.qos = payloadMessage.qos;
        storedMessage.payloadMessage.destinationName = payloadMessage.destinationName;
        if (payloadMessage.duplicate) {
          storedMessage.payloadMessage.duplicate = true;
        }
        if (payloadMessage.retained) {
          storedMessage.payloadMessage.retained = true;
        }

        // Add a sequence number to sent messages.
        if (prefix.indexOf('Sent:') === 0) {
          if (wireMessage.sequence === undefined) {
            wireMessage.sequence = ++this._sequence;
          }
          storedMessage.sequence = wireMessage.sequence;
        }
        break;

      default:
        throw Error(format(ERROR.INVALID_STORED_DATA, [prefix + this._localKey + messageIdentifier, storedMessage]));
    }
    this.storage.setItem(prefix + this._localKey + messageIdentifier, JSON.stringify(storedMessage));
  }

  restore(key: string) {
    const value = this.storage.getItem(key);
    const storedMessage = JSON.parse(value);

    let wireMessage: PublishMessage;

    switch (storedMessage.type) {
      case MESSAGE_TYPE.PUBLISH:
        // Replace the payload message with a Message object.
        let hex = storedMessage.payloadMessage.payloadHex;
        const buffer = new ArrayBuffer((hex.length) / 2);
        const byteStream = new Uint8Array(buffer);
        let i = 0;
        while (hex.length >= 2) {
          const x = parseInt(hex.substring(0, 2), 16);
          hex = hex.substring(2, hex.length);
          byteStream[i++] = x;
        }
        const payloadMessage = new Message(byteStream);

        payloadMessage.qos = storedMessage.payloadMessage.qos;
        payloadMessage.destinationName = storedMessage.payloadMessage.destinationName;
        if (storedMessage.payloadMessage.duplicate) {
          payloadMessage.duplicate = true;
        }
        if (storedMessage.payloadMessage.retained) {
          payloadMessage.retained = true;
        }
        wireMessage = new PublishMessage(payloadMessage, storedMessage.messageIdentifier);

        break;

      default:
        throw Error(format(ERROR.INVALID_STORED_DATA, [key, value]));
    }

    if (key.indexOf('Sent:' + this._localKey) === 0) {
      wireMessage.payloadMessage.duplicate = true;
      invariant(wireMessage.messageIdentifier, format(ERROR.INVALID_STATE, ['Stored WireMessage with no messageIdentifier']));
      this._outboundMessagesInFlight[wireMessage.messageIdentifier.toString()] = wireMessage;
    } else if (key.indexOf('Received:' + this._localKey) === 0) {
      invariant(wireMessage.messageIdentifier, format(ERROR.INVALID_STATE, ['Stored WireMessage with no messageIdentifier']));
      this._receivedMessagesAwaitingAckConfirm[wireMessage.messageIdentifier.toString()] = wireMessage;
    }
  }

  _process_queue() {
    // Process messages in order they were added
    // Send all queued messages down socket connection
    const socket = this.socket;

    // Consume each message and remove it from the queue
    let wireMessage;
    while ((wireMessage = this._messagesAwaitingDispatch.shift())) {
      this._trace('Client._socketSend', wireMessage);
      socket && socket.send(wireMessage.encode());
      wireMessage.onDispatched && wireMessage.onDispatched();
    }

    this.sendPinger && this.sendPinger.reset();
  }

  /**
   * Expect an ACK response for this message. Add message to the set of in progress
   * messages and set an unused identifier in this message.
   * @ignore
   */
  _requires_ack(wireMessage: WireMessage | PublishMessage) {
    const messageCount = Object.keys(this._outboundMessagesInFlight).length;
    if (messageCount > this.maxOutboundInFlight) {
      throw Error('Too many messages:' + messageCount);
    }

    while (this._outboundMessagesInFlight[this._nextMessageId.toString()] !== undefined) {
      this._nextMessageId++;
    }
    wireMessage.messageIdentifier = this._nextMessageId;
    this._outboundMessagesInFlight[wireMessage.messageIdentifier.toString()] = wireMessage;
    if (wireMessage instanceof PublishMessage) {
      this.store('Sent:', wireMessage);
    }
    if (this._nextMessageId === this.maxOutboundInFlight) {
      // Next time we will search for the first unused ID up from 1
      this._nextMessageId = 1;
    }
  }

  _deframeMessages(data: ArrayBuffer): ?Array<(WireMessage | PublishMessage)> {
    let byteArray = new Uint8Array(data);
    if (this.receiveBuffer) {
      const receiveBufferLength = this.receiveBuffer.length;
      const newData = new Uint8Array(receiveBufferLength + byteArray.length);
      newData.set(this.receiveBuffer);
      newData.set(byteArray, receiveBufferLength);
      byteArray = newData;
      this.receiveBuffer = null;
    }
    try {
      let offset = 0;
      let messages: (WireMessage | PublishMessage)[] = [];
      while (offset < byteArray.length) {
        const result = decodeMessage(byteArray, offset);
        const wireMessage = result[0];
        offset = result[1];
        if (wireMessage) {
          messages.push(wireMessage);
        } else {
          break;
        }
      }
      if (offset < byteArray.length) {
        this.receiveBuffer = byteArray.subarray(offset);
      }
      return messages;
    } catch (error) {
      this._disconnected(ERROR.INTERNAL_ERROR.code, format(ERROR.INTERNAL_ERROR, [error.message, error.stack.toString()]));
    }
  }

  _handleMessage(wireMessage: WireMessage | PublishMessage) {

    this._trace('Client._handleMessage', wireMessage);
    const connectOptions = this.connectOptions;
    invariant(connectOptions, format(ERROR.INVALID_STATE, ['_handleMessage invoked but connectOptions not set']));

    try {
      if (wireMessage instanceof PublishMessage) {
        this._receivePublish(wireMessage);
        return;
      }

      let sentMessage: WireMessage | PublishMessage, receivedMessage: WireMessage | PublishMessage, messageIdentifier;

      switch (wireMessage.type) {
        case MESSAGE_TYPE.CONNACK:
          clearTimeout(this._connectTimeout);

          // If we have started using clean session then clear up the local state.
          if (connectOptions.cleanSession) {
            Object.keys(this._outboundMessagesInFlight).forEach((key) => {
              sentMessage = this._outboundMessagesInFlight[key];
              invariant(messageIdentifier = sentMessage.messageIdentifier, format(ERROR.INVALID_STATE, ['Stored WireMessage with no messageIdentifier']));
              this.storage.removeItem('Sent:' + this._localKey + messageIdentifier);
            });
            this._outboundMessagesInFlight = {};

            Object.keys(this._receivedMessagesAwaitingAckConfirm).forEach((key) => {
              receivedMessage = this._receivedMessagesAwaitingAckConfirm[key];
              invariant(messageIdentifier = receivedMessage.messageIdentifier, format(ERROR.INVALID_STATE, ['Stored WireMessage with no messageIdentifier']));
              this.storage.removeItem('Received:' + this._localKey + messageIdentifier);
            });
            this._receivedMessagesAwaitingAckConfirm = {};
          }
          // Client connected and ready for business.
          if (wireMessage.returnCode === 0) {
            this.connected = true;
          } else {
            this._disconnected(ERROR.CONNACK_RETURNCODE.code, format(ERROR.CONNACK_RETURNCODE, [wireMessage.returnCode, CONNACK_RC[wireMessage.returnCode]]));
            break;
          }

          // Resend messages. Sort sentMessages into the original sent order.
          const sequencedMessages = Object.keys(this._outboundMessagesInFlight).map(key => this._outboundMessagesInFlight[key]).sort((a, b) => (a.sequence || 0) - (b.sequence || 0));

          sequencedMessages.forEach((sequencedMessage) => {
            invariant(messageIdentifier = sequencedMessage.messageIdentifier, format(ERROR.INVALID_STATE, ['PUBREL WireMessage with no messageIdentifier']));
            if (sequencedMessage instanceof PublishMessage && sequencedMessage.pubRecReceived) {
              this._scheduleMessage(new WireMessage(MESSAGE_TYPE.PUBREL, { messageIdentifier }));
            } else {
              this._scheduleMessage(sequencedMessage);
            }
          });

          // Execute the connectOptions.onSuccess callback if there is one.
          connectOptions.onSuccess && connectOptions.onSuccess();

          // Process all queued messages now that the connection is established.
          this._process_queue();
          break;

        case MESSAGE_TYPE.PUBACK:
          invariant(messageIdentifier = wireMessage.messageIdentifier, format(ERROR.INVALID_STATE, ['PUBACK WireMessage with no messageIdentifier']));
          sentMessage = this._outboundMessagesInFlight[messageIdentifier.toString()];
          // If this is a re flow of a PUBACK after we have restarted receivedMessage will not exist.
          if (sentMessage) {
            delete this._outboundMessagesInFlight[messageIdentifier.toString()];
            this.storage.removeItem('Sent:' + this._localKey + messageIdentifier);
            if (this.onMessageDelivered && sentMessage instanceof PublishMessage) {
              this.onMessageDelivered(sentMessage.payloadMessage);
            }
          }
          break;

        case MESSAGE_TYPE.PUBREC:
          invariant(messageIdentifier = wireMessage.messageIdentifier, format(ERROR.INVALID_STATE, ['PUBREC WireMessage with no messageIdentifier']));
          sentMessage = this._outboundMessagesInFlight[messageIdentifier.toString()];
          // If this is a re flow of a PUBREC after we have restarted receivedMessage will not exist.
          if (sentMessage && sentMessage instanceof PublishMessage) {
            sentMessage.pubRecReceived = true;
            const pubRelMessage = new WireMessage(MESSAGE_TYPE.PUBREL, { messageIdentifier });
            this.store('Sent:', sentMessage);
            this._scheduleMessage(pubRelMessage);
          }
          break;

        case MESSAGE_TYPE.PUBREL:
          invariant(messageIdentifier = wireMessage.messageIdentifier, format(ERROR.INVALID_STATE, ['PUBREL WireMessage with no messageIdentifier']));
          receivedMessage = this._receivedMessagesAwaitingAckConfirm[messageIdentifier.toString()];
          this.storage.removeItem('Received:' + this._localKey + messageIdentifier);
          // If this is a re flow of a PUBREL after we have restarted receivedMessage will not exist.
          if (receivedMessage && receivedMessage instanceof PublishMessage) {
            this._receiveMessage(receivedMessage);
            delete this._receivedMessagesAwaitingAckConfirm[messageIdentifier.toString()];
          }
          // Always flow PubComp, we may have previously flowed PubComp but the server lost it and restarted.
          const pubCompMessage = new WireMessage(MESSAGE_TYPE.PUBCOMP, { messageIdentifier });
          this._scheduleMessage(pubCompMessage);
          break;

        case MESSAGE_TYPE.PUBCOMP:
          invariant(messageIdentifier = wireMessage.messageIdentifier, format(ERROR.INVALID_STATE, ['PUBCOMP WireMessage with no messageIdentifier']));
          sentMessage = this._outboundMessagesInFlight[messageIdentifier.toString()];
          delete this._outboundMessagesInFlight[messageIdentifier.toString()];
          this.storage.removeItem('Sent:' + this._localKey + messageIdentifier);
          if (this.onMessageDelivered && sentMessage instanceof PublishMessage) {
            this.onMessageDelivered(sentMessage.payloadMessage);
          }
          break;

        case MESSAGE_TYPE.SUBACK:
          invariant(messageIdentifier = wireMessage.messageIdentifier, format(ERROR.INVALID_STATE, ['SUBACK WireMessage with no messageIdentifier']));
          sentMessage = this._outboundMessagesInFlight[messageIdentifier.toString()];
          if (sentMessage) {
            if (sentMessage.timeOut) {
              clearTimeout(sentMessage.timeOut);
            }
            invariant(wireMessage.returnCode instanceof Uint8Array, format(ERROR.INVALID_STATE, ['SUBACK WireMessage with invalid returnCode']));
            // This will need to be fixed when we add multiple topic support
            if (wireMessage.returnCode[0] === 0x80) {
              if ((sentMessage instanceof WireMessage) && sentMessage.onFailure) {
                sentMessage.onFailure(new Error('Suback error'));
              }
            } else if ((sentMessage instanceof WireMessage) && sentMessage.subAckReceived) {
              sentMessage.subAckReceived(wireMessage.returnCode[0]);
            }
            delete this._outboundMessagesInFlight[messageIdentifier.toString()];
          }
          break;

        case MESSAGE_TYPE.UNSUBACK:
          invariant(messageIdentifier = wireMessage.messageIdentifier, format(ERROR.INVALID_STATE, ['UNSUBACK WireMessage with no messageIdentifier']));
          sentMessage = this._outboundMessagesInFlight[messageIdentifier.toString()];
          if (sentMessage && (sentMessage instanceof WireMessage) && sentMessage.type === MESSAGE_TYPE.UNSUBSCRIBE) {
            if (sentMessage.timeOut) {
              clearTimeout(sentMessage.timeOut);
            }
            sentMessage.unSubAckReceived && sentMessage.unSubAckReceived();
            delete this._outboundMessagesInFlight[messageIdentifier.toString()];
          }

          break;

        case MESSAGE_TYPE.PINGRESP:
          // We don't care whether the server is still there (yet)
          this.sendPinger && this.sendPinger.reset();
          break;

        case MESSAGE_TYPE.DISCONNECT:
          // Clients do not expect to receive disconnect packets.
          this._disconnected(ERROR.INVALID_MQTT_MESSAGE_TYPE.code, format(ERROR.INVALID_MQTT_MESSAGE_TYPE, [wireMessage.type]));
          break;

        default:
          this._disconnected(ERROR.INVALID_MQTT_MESSAGE_TYPE.code, format(ERROR.INVALID_MQTT_MESSAGE_TYPE, [wireMessage.type]));
      }
    } catch (error) {
      this._disconnected(ERROR.INTERNAL_ERROR.code, format(ERROR.INTERNAL_ERROR, [error.message, error.stack.toString()]));
    }
  }

  /** @ignore */
  _socketSend(wireMessage: WireMessage) {
    this._trace('Client._socketSend', wireMessage);
    this.socket && this.socket.send(wireMessage.encode());
    // We have proved to the server we are alive.
    this.sendPinger && this.sendPinger.reset();
  }

  /** @ignore */
  _receivePublish(wireMessage: PublishMessage) {
    const { payloadMessage, messageIdentifier } = wireMessage;
    invariant(payloadMessage, format(ERROR.INVALID_STATE, ['PUBLISH WireMessage with no payloadMessage']));
    switch (payloadMessage.qos) {
      case 0:
        this._receiveMessage(wireMessage);
        break;

      case 1:
        invariant(messageIdentifier, format(ERROR.INVALID_STATE, ['QoS 1 WireMessage with no messageIdentifier']));
        this._scheduleMessage(new WireMessage(MESSAGE_TYPE.PUBACK, { messageIdentifier }));
        this._receiveMessage(wireMessage);
        break;

      case 2:
        invariant(messageIdentifier, format(ERROR.INVALID_STATE, ['QoS 2 WireMessage with no messageIdentifier']));
        this._receivedMessagesAwaitingAckConfirm[messageIdentifier.toString()] = wireMessage;
        this.store('Received:', wireMessage);
        this._scheduleMessage(new WireMessage(MESSAGE_TYPE.PUBREC, { messageIdentifier }));
        break;

      default:
        throw Error('Invaild qos=' + payloadMessage.qos);
    }
  }

  /** @ignore */
  _receiveMessage(wireMessage: PublishMessage) {
    if (this.onMessageArrived) {
      this.onMessageArrived(wireMessage.payloadMessage);
    }
  }

  /**
   * Client has disconnected either at its own request or because the server
   * or network disconnected it. Remove all non-durable state.
   * @param {number} [errorCode] the error number.
   * @param {string} [errorText] the error text.
   * @ignore
   */
  _disconnected(errorCode?: number, errorText?: string) {
    this._trace('Client._disconnected', errorCode, errorText);

    this.sendPinger && this.sendPinger.cancel();
    if (this._connectTimeout) {
      clearTimeout(this._connectTimeout);
    }
    // Clear message buffers.
    this._messagesAwaitingDispatch = [];

    if (this.socket) {
      // Cancel all socket callbacks so that they cannot be driven again by this socket.
      this.socket.onopen = () => null;
      this.socket.onmessage = () => null;
      this.socket.onerror = () => null;
      this.socket.onclose = () => null;
      if (this.socket.readyState === 1) {
        this.socket.close();
      }
      this.socket = null;
    }

    if (errorCode === undefined) {
      errorCode = ERROR.OK.code;
      errorText = format(ERROR.OK);
    }

    // Run any application callbacks last as they may attempt to reconnect and hence create a new socket.
    if (this.connected) {
      this.connected = false;
      // Execute the onConnectionLost callback if there is one, and we were connected.
      this.onConnectionLost && this.onConnectionLost({ errorCode: errorCode, errorMessage: errorText });
    } else {
      // Otherwise we never had a connection, so indicate that the connect has failed.
      if (this.connectOptions && this.connectOptions.onFailure) {
        this.connectOptions.onFailure(new Error(errorText));
      }
    }
  }

  /** @ignore */
  _trace(...args: any) {
    // Pass trace message back to client's callback function
    const traceFunction = this.traceFunction;
    if (traceFunction) {
      traceFunction({ severity: 'Debug', message: args.map(a => JSON.stringify(a)).join('')});
    }

    //buffer style trace
    if (this._traceBuffer !== null) {
      for (let i = 0, max = args.length; i < max; i++) {
        if (this._traceBuffer.length === this._MAX_TRACE_ENTRIES) {
          this._traceBuffer.shift();
        }
        if (i === 0 || typeof args[i] === 'undefined') {
          this._traceBuffer.push(args[i]);
        } else {
          this._traceBuffer.push('  ' + JSON.stringify(args[i]));
        }
      }
    }
  }
}

export default ClientImplementation;
