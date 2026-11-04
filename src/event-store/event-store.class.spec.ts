import 'reflect-metadata';
import { Test } from '@nestjs/testing';
import { EventStore } from './event-store.class';

function createConnectionSpy() {
  return {
    on: jest.fn(),
    removeAllListeners: jest.fn(),
    connect: jest.fn().mockResolvedValue(undefined),
    close: jest.fn(),
  };
}

function createStoreWithConnection() {
  const store = new EventStore({} as any, {} as any);
  const connection = createConnectionSpy();
  // Replace the connection created by connect() with a controllable spy.
  store.connection = connection as any;
  return { store, connection };
}

describe('EventStore shutdown', () => {
  it('closes the connection when the application shuts down', async () => {
    const { store, connection } = createStoreWithConnection();

    const moduleRef = await Test.createTestingModule({
      providers: [{ provide: EventStore, useFactory: () => store }],
    }).compile();

    const app = moduleRef.createNestApplication();
    await app.init();
    await app.close();

    expect(connection.removeAllListeners).toHaveBeenCalledTimes(1);
    expect(connection.close).toHaveBeenCalledTimes(1);
    expect(store.connection).toBeUndefined();
  });

  it('is idempotent when shutdown runs more than once', () => {
    const { store, connection } = createStoreWithConnection();

    store.onApplicationShutdown();
    store.onApplicationShutdown();

    expect(connection.close).toHaveBeenCalledTimes(1);
    expect(store.connection).toBeUndefined();
  });
});
