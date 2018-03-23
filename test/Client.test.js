import { Client } from '../';
import { uri } from './.support';

import { w3cwebsocket as webSocket } from 'websocket';
import { LocalStorage } from 'node-localstorage';

const storage = new LocalStorage('./tmp');

describe('client-uris', function () {

  test('should create a new client with a uri', function () {
    const client = new Client({
      uri,
      clientId: 'testclientid',
      webSocket,
      storage
    });

    expect(client).not.toBe(null);
    expect(client.uri).toBe(uri);
  });

  test('should create a new client with a uri', function () {
    const client = new Client({
      uri: 'ws://localhost:9000',
      clientId: 'testclientid',
      webSocket,
      storage
    });

    expect(client).not.toBe(null);
    expect(client.uri).toBe('ws://localhost:9000');
  });

  test('should fail to create a new client with an invalid ws uri', function () {
    let client = null;
    let error;
    try {
      client = new Client({ uri: 'http://example.com', clientId: 'testclientid', webSocket, storage });
    } catch (err) {
      error = err;
    }
    expect(client).toBe(null);
    expect(error).not.toBe(null);
  });

  /*
   // We don't yet expose setting the path element with the arrays of hosts/ports
   // If you want a path other than /mqtt, you need to use the array of hosts-as-uris.
   // Leaving this test here to remember this fact in case we add an array of paths to connopts
   it('should connect and disconnect to a server using connectoptions hosts and ports', function() {
   client = new Paho.MQTT.Client(testServer, testPort, "testclientid");
   expect(client).not.toBe(null);

   client.onMessageArrived = messageArrived;
   client.onConnectionLost = onDisconnect;

   runs(function() {
   client.connect({onSuccess:onConnect,hosts:[host],ports:[port]});
   });

   waitsFor(function() {
   return connected;
   }, "the client should connect", 10000);

   runs(function() {
   expect(connected).toBe(true);
   });
   runs(function() {
   client.disconnect();
   });
   waitsFor(function() {
   return !connected;
   }, "the client should disconnect",1000);
   runs(function() {
   expect(connected).toBe(false);
   expect(disconnectError).not.toBe(null);
   expect(disconnectError.errorCode).toBe(0);
   });
   });
   */
});
