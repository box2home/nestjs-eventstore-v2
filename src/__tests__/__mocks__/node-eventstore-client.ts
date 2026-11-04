/**
 * Jest manual mock for `node-eventstore-client`.
 *
 * The real package mutates `require.cache` at import time
 * (`src/utils/modules.js` -> `uncache`/`recache`). Jest's `require.cache` is a
 * Proxy whose `deleteProperty` trap is a no-op, so `uncache()` recurses until
 * the stack overflows:
 *
 *   RangeError: Maximum call stack size exceeded
 *     at uncache (node-eventstore-client/src/utils/modules.js:7:7)
 *
 * Mapping the module away in `jest.config` keeps the client out of unit tests.
 */
export const createEventData = (
  eventId: string,
  eventType: string,
  isJson: boolean,
  data: Buffer,
  metadata?: Buffer,
) => ({ eventId, eventType, isJson, data, metadata });

const noop = () => undefined;

export const createConnection = () => ({
  on: noop,
  removeAllListeners: noop,
  connect: async () => undefined,
  close: noop,
  appendToStream: async () => ({}),
  setStreamMetadataRaw: async () => ({}),
  subscribeToStreamFrom: () => ({ isLive: true }),
  connectToPersistentSubscription: async () => ({ isLive: true }),
});

export class TcpEndPoint {}
export class ConnectionSettings {}
export class EventStoreNodeConnection {}
