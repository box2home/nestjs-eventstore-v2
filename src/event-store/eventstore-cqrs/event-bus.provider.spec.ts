import 'reflect-metadata';
import { Test } from '@nestjs/testing';
import { EventsHandler } from '@nestjs/cqrs';
import { EventBusProvider } from './event-bus.provider';
import { EventStoreCqrsModule } from './eventstore-cqrs.module';
import { EventStore } from '../event-store.class';

class PingEvent {
  constructor(public readonly id: string) {}
}

let handled: string | null = null;

@EventsHandler(PingEvent)
class PingHandler {
  handle(event: PingEvent) {
    handled = event.id;
  }
}

function createFakeEventStore() {
  const appendToStream = jest.fn().mockResolvedValue({});
  const setStreamMetadataRaw = jest.fn().mockResolvedValue({});

  const connection = {
    appendToStream,
    setStreamMetadataRaw,
    subscribeToStreamFrom: jest.fn(),
    connectToPersistentSubscription: jest.fn().mockResolvedValue({ isLive: true }),
    close: jest.fn(),
    removeAllListeners: jest.fn(),
  };

  return {
    appendToStream,
    setStreamMetadataRaw,
    store: {
      getConnection: () => connection,
      close: jest.fn(),
    },
  };
}

async function createTestingModule(fake: ReturnType<typeof createFakeEventStore>) {
  return Test.createTestingModule({
    imports: [
      EventStoreCqrsModule.forRootAsync(
        { useFactory: () => ({}) },
        {
          subscriptions: [],
          eventInstantiators: {
            PingEvent: (id: string) => new PingEvent(id),
          },
        },
      ),
    ],
    providers: [PingHandler],
  })
    .overrideProvider(EventStore)
    .useValue(fake.store)
    .compile();
}

describe('EventBusProvider', () => {
  beforeEach(() => {
    handled = null;
  });

  it('discovers @EventsHandler providers as InstanceWrapper and dispatches', async () => {
    const fake = createFakeEventStore();
    const moduleRef = await createTestingModule(fake);
    const app = moduleRef.createNestApplication();
    await app.init();

    const bus = app.get(EventBusProvider);
    (bus as any).subject$.next(new PingEvent('p1'));

    expect(handled).toBe('p1');

    await app.close();
  });

  it('publishWithMaxAge appends with ExpectedVersion.any (-2)', async () => {
    const fake = createFakeEventStore();
    const moduleRef = await createTestingModule(fake);

    const bus = moduleRef.get(EventBusProvider);
    await bus.publishWithMaxAge(new PingEvent('p2'), 'stream-1');

    expect(fake.appendToStream).toHaveBeenCalledWith(
      'stream-1',
      -2,
      expect.any(Array),
    );
    expect(fake.setStreamMetadataRaw).toHaveBeenCalledWith(
      'stream-1',
      -2,
      expect.any(Buffer),
    );
  });
});
