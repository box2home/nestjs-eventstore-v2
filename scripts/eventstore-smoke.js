#!/usr/bin/env node
/**
 * EventStore TCP smoke test for Node 24.
 *
 * Validates the real protocol path used by this library:
 *   connect -> append -> read -> EventStoreBus.publish -> publishWithMaxAge -> close
 *
 * `node-eventstore-client` is pure JavaScript over the legacy TCP API
 * (port 1113) and protobufjs - there are no native bindings and no gRPC.
 *
 * The script waits for a successful read before writing: EventStoreDB opens
 * TCP 1113 while it is still recovering chunks, so a TCP accept alone is not
 * a readiness signal.
 *
 * Usage:
 *   npm run build
 *   docker run -d --name nes-smoke -p 1113:1113 -p 2113:2113 \
 *     eventstore/eventstore:release-5.0.8
 *   node scripts/eventstore-smoke.js
 *   docker rm -f nes-smoke
 *
 * Environment:
 *   EVENTSTORE_HOST (default 127.0.0.1)
 *   EVENTSTORE_PORT (default 1113)
 */
const { EventStore, EventStoreBus } = require('../dist');
const { createEventData } = require('node-eventstore-client');
const { v4 } = require('uuid');
const { Subject } = require('rxjs');

const HOST = process.env.EVENTSTORE_HOST || '127.0.0.1';
const PORT = Number(process.env.EVENTSTORE_PORT || 1113);

class SmokeEvent {
  constructor(id) {
    this.id = id;
  }
}

function waitFor(predicate, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    const startedAt = Date.now();
    const tick = () => {
      if (predicate()) return resolve();
      if (Date.now() - startedAt > timeoutMs) {
        return reject(new Error(`timeout after ${timeoutMs}ms`));
      }
      setTimeout(tick, 100);
    };
    tick();
  });
}

/**
 * EventStoreDB starts listening on TCP 1113 before it has finished replaying
 * and verifying chunks. A write issued in that window is retried by the
 * client until it hits the retry limit, so a TCP accept (or even a read of a
 * missing stream) is not a readiness signal. Only a successful write is.
 *
 * `store.getConnection()` is read on every attempt because the library
 * replaces the connection object when it reconnects.
 */
async function waitForServerReady(store, timeoutMs = 180000) {
  const probeStream = 'smoke-readiness';
  const startedAt = Date.now();
  let attempt = 0;

  for (;;) {
    const connection = store.getConnection();
    try {
      if (!connection) {
        throw new Error('no active connection');
      }
      await connection.appendToStream(probeStream, -2, [
        createEventData(
          v4(),
          'SmokeReady',
          true,
          Buffer.from(JSON.stringify({ ready: true })),
        ),
      ]);
      return;
    } catch (error) {
      if (Date.now() - startedAt >= timeoutMs) {
        throw new Error(
          `server not ready after ${timeoutMs}ms: ${error.message}`,
        );
      }
      const delay = Math.min(500 * 2 ** attempt, 5000);
      attempt += 1;
      console.log(
        `   waiting for EventStore recovery (${error.message.split('\n')[0]}); retry ${attempt} in ${delay}ms`,
      );
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
}

let store;

(async () => {
  store = new EventStore({}, { host: HOST, port: PORT });
  await waitFor(() => store.isConnected);

  await waitForServerReady(store);
  console.log('1) connect ......................... PASS');

  const connection = store.getConnection();
  const directStream = `smoke-direct-${v4()}`;
  await connection.appendToStream(directStream, -2, [
    createEventData(
      v4(),
      'SmokeEvent',
      true,
      Buffer.from(JSON.stringify({ ok: true })),
    ),
  ]);
  console.log('2) direct appendToStream ........... PASS');

  const directRead = await connection.readStreamEventsForward(
    directStream,
    0,
    10,
    false,
  );
  if (!directRead.events || directRead.events.length !== 1) {
    throw new Error('direct read did not return the appended event');
  }
  console.log('3) direct readStreamEventsForward .. PASS');

  const bus = new EventStoreBus(store, new Subject(), {
    subscriptions: [],
    eventInstantiators: {},
  });

  const busStream = `smoke-bus-${v4()}`;
  await bus.publish(new SmokeEvent('bus-1'), busStream);
  const busRead = await connection.readStreamEventsForward(
    busStream,
    0,
    10,
    false,
  );
  if (!busRead.events || busRead.events.length !== 1) {
    throw new Error('EventStoreBus.publish did not persist the event');
  }
  console.log('4) EventStoreBus.publish ........... PASS');

  const maxAgeStream = `smoke-maxage-${v4()}`;
  await bus.publishWithMaxAge(new SmokeEvent('age-1'), maxAgeStream, 30);
  const maxAgeRead = await connection.readStreamEventsForward(
    maxAgeStream,
    0,
    10,
    false,
  );
  if (!maxAgeRead.events || maxAgeRead.events.length !== 1) {
    throw new Error('publishWithMaxAge did not persist the event');
  }
  console.log('5) publishWithMaxAge ............... PASS');

  store.onApplicationShutdown();
  if (store.connection !== undefined) {
    throw new Error('onApplicationShutdown did not close the connection');
  }
  console.log('6) onApplicationShutdown ........... PASS');

  console.log(`ALL TCP CHECKS PASSED ON ${process.version}`);
})().catch((error) => {
  console.error(`FAIL: ${error.message}`);
  if (/timeout after/.test(error.message)) {
    console.error(
      `Hint: is EventStoreDB running and reachable at ${HOST}:${PORT}?`,
    );
  }
  process.exitCode = 1;
}).finally(() => {
  if (store) store.onApplicationShutdown();
});
