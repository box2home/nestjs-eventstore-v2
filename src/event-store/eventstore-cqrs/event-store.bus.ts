import { IEvent } from '@nestjs/cqrs';
import { Subject } from 'rxjs';
import {
  EventData,
  createEventData,
  EventStorePersistentSubscription,
  ResolvedEvent,
  EventStoreCatchUpSubscription,
} from 'node-eventstore-client';
import { v4 } from 'uuid';
import { Logger } from '@nestjs/common';
import { EventStore } from '../event-store.class';
import {
  EventStoreBusConfig,
  EventStoreSubscriptionType,
  EventStorePersistentSubscription as ESPersistentSubscription,
  EventStoreCatchupSubscription as ESCatchUpSubscription,
} from './event-bus.provider';

export interface IEventConstructors {
  [key: string]: (...args: any[]) => IEvent;
}

interface ExtendedCatchUpSubscription extends EventStoreCatchUpSubscription {
  isLive: boolean | undefined;
}

interface ExtendedPersistentSubscription
  extends EventStorePersistentSubscription {
  isLive: boolean | undefined;
}

export class EventStoreBus {
  private eventHandlers: IEventConstructors;
  private logger = new Logger('EventStoreBus');
  private catchupSubscriptions: ExtendedCatchUpSubscription[] = [];
  private catchupSubscriptionsCount: number;
  private readonly configuredStreams = new Set<string>();
  private persistentSubscriptions: ExtendedPersistentSubscription[] = [];
  private persistentSubscriptionsCount: number;
  private retryTimers: NodeJS.Timeout[] = [];

  constructor(
    private eventStore: EventStore,
    private subject$: Subject<IEvent>,
    config: EventStoreBusConfig,
  ) {
    this.addEventHandlers(config.eventInstantiators);

    const catchupSubscriptions = config.subscriptions.filter((sub) => {
      return sub.type === EventStoreSubscriptionType.CatchUp;
    });

    const persistentSubscriptions = config.subscriptions.filter((sub) => {
      return sub.type === EventStoreSubscriptionType.Persistent;
    });

    this.subscribeToCatchUpSubscriptions(
      catchupSubscriptions as ESCatchUpSubscription[],
    );

    this.subscribeToPersistentSubscriptions(
      persistentSubscriptions as ESPersistentSubscription[],
    );
  }

  async subscribeToPersistentSubscriptions(
    subscriptions: ESPersistentSubscription[],
  ) {
    this.persistentSubscriptionsCount = subscriptions.length;
    this.persistentSubscriptions = await Promise.all(
      subscriptions.map(async (subscription) => {
        return await this.subscribeToPersistentSubscription(
          subscription.stream,
          subscription.persistentSubscriptionName,
        );
      }),
    );
  }

  subscribeToCatchUpSubscriptions(subscriptions: ESCatchUpSubscription[]) {
    this.catchupSubscriptionsCount = subscriptions.length;
    this.catchupSubscriptions = subscriptions.map((subscription) => {
      return this.subscribeToCatchupSubscription(subscription.stream);
    });
  }

  get allCatchUpSubscriptionsLive(): boolean {
    const initialized =
      this.catchupSubscriptions.length === this.catchupSubscriptionsCount;
    return (
      initialized &&
      this.catchupSubscriptions.every((subscription) => {
        return !!subscription && subscription.isLive;
      })
    );
  }

  get allPersistentSubscriptionsLive(): boolean {
    const initialized =
      this.persistentSubscriptions.length === this.persistentSubscriptionsCount;
    return (
      initialized &&
      this.persistentSubscriptions.every((subscription) => {
        return !!subscription && subscription.isLive;
      })
    );
  }

  get isLive(): boolean {
    return (
      this.allCatchUpSubscriptionsLive && this.allPersistentSubscriptionsLive
    );
  }

  async publish(event: IEvent, stream?: string) {
    const payload: EventData = createEventData(
      v4(),
      event.constructor.name,
      true,
      Buffer.from(JSON.stringify(event)),
    );

    try {
      await this.eventStore.getConnection().appendToStream(stream, -2, [payload]);
    } catch (error) {
      const err = error as Error;
      this.logger.error(err.message, err.stack);
    }
  }

  async publishWithMaxAge(
    event: any,
    streamName: string,
    maxAgeSeconds = 60,
  ): Promise<void> {
    try {
      const payload: EventData = createEventData(
        v4(),
        event.constructor.name,
        true,
        Buffer.from(JSON.stringify(event)),
      );

      const conn = this.eventStore.getConnection();

      // -2 is ExpectedVersion.any; -1 (noStream) would reject the append as
      // soon as the stream already exists.
      await conn.appendToStream(streamName, -2, [payload]);

      if (!this.configuredStreams.has(streamName)) {
        const metadata = Buffer.from(
          JSON.stringify({ $maxAge: maxAgeSeconds }),
          'utf8',
        );
        await conn.setStreamMetadataRaw(streamName, -2, metadata);
        this.configuredStreams.add(streamName);
        this.logger.log(
          `Configured stream ${streamName} with $maxAge = ${maxAgeSeconds}`,
        );
      }
    } catch (error) {
      // Never reject: callers may fire-and-forget, and an unhandled rejection
      // terminates the process on Node >= 15 (including Node 24).
      const err = error as Error;
      this.logger.error(
        `[publishWithMaxAge][${streamName}] ${err.message}`,
        err.stack,
      );
    }
  }

  async publishAll(events: IEvent[], stream?: string) {
    try {
      await this.eventStore.getConnection().appendToStream(stream, -2, (events || []).map(
        (event: IEvent) => createEventData(
          v4(),
          event.constructor.name,
          true,
          Buffer.from(JSON.stringify(event)),
        ),
      ));
    } catch (error) {
      const err = error as Error;
      this.logger.error(err.message, err.stack);
    }
  }

  subscribeToCatchupSubscription(stream: string): ExtendedCatchUpSubscription {
    this.logger.log(`Catching up and subscribing to stream ${stream}!`);
    try {
      return this.eventStore.getConnection().subscribeToStreamFrom(
        stream,
        0,
        true,
        (sub, payload) => this.onEvent(sub, payload),
        subscription =>
          this.onLiveProcessingStarted(
            subscription as ExtendedCatchUpSubscription,
          ),
        (sub, reason, error) =>
          this.onDropped(sub as ExtendedCatchUpSubscription, reason, error),
      ) as ExtendedCatchUpSubscription;
    } catch (error) {
      const err = error as Error;
      this.logger.error(err.message, err.stack);
    }
  }

  async subscribeToPersistentSubscription(
    stream: string,
    subscriptionName: string,
  ): Promise<ExtendedPersistentSubscription> {
    try {
      const resolved = (await this.eventStore.getConnection().connectToPersistentSubscription(
        stream,
        subscriptionName,
        (sub, payload) => this.onEvent(sub, payload),
        (sub, reason, error) =>
          this.onDropped(sub as ExtendedPersistentSubscription, reason, error),
      )) as ExtendedPersistentSubscription;
      this.logger.log(`Connection to persistent subscription ${subscriptionName} on stream ${stream} established!`);
      resolved.isLive = true;
      return resolved;
    } catch (error) {
      const err = error as Error;
      this.logger.error(
        `[${stream}][${subscriptionName}] ${err.message}`,
        err.stack,
      );
      this.reSubscribeToPersistentSubscription(stream, subscriptionName);
    }
  }

  async onEvent(
    _subscription:
      | EventStorePersistentSubscription
      | EventStoreCatchUpSubscription,
    payload: ResolvedEvent,
  ) {
    const { event } = payload;
    if (!event) {
      this.logger.error(
        'Event could not be resolved: payload.event is null (link event is deleted or not yet propagated)',
      );
      return;
    }
    if ((payload.link !== null && !payload.isResolved) || !event.isJson) {
      this.logger.error(`${event.eventType} could not be resolved!`);
      return;
    }
    const handler = this.eventHandlers[event.eventType];
    if (!handler) {
      this.logger.error(`${event.eventType} could not be handled!`);
      return;
    }
    const data = Object.values(JSON.parse(event.data.toString()));
    this.subject$.next(handler(...data));
  }

  onDropped(
    subscription: ExtendedPersistentSubscription | ExtendedCatchUpSubscription,
    _reason: string,
    error: Error,
  ) {
    subscription.isLive = false;
    this.logger.error(error.message, error.stack);
    if((subscription as any)._subscriptionId!=undefined)
      this.reSubscribeToPersistentSubscription((subscription as any)._streamId,(subscription as any)._subscriptionId)
  }

  reSubscribeToPersistentSubscription(
    stream: string,
    subscriptionName: string,
  ){
    this.logger.warn(`connecting to subscription ${subscriptionName} ${stream}. Retrying...`);
    const timer = setTimeout(
      () => {
        this.retryTimers = this.retryTimers.filter(t => t !== timer);
        this.subscribeToPersistentSubscription(stream, subscriptionName);
      },
      3000,
    );
    this.retryTimers.push(timer);
    if (typeof timer.unref === 'function') {
      timer.unref();
    }
  }

  /**
   * Clears pending reconnect timers. Called from `EventBusProvider.onModuleDestroy`
   * so a shut-down application does not keep the event loop alive.
   */
  dispose() {
    this.retryTimers.forEach(timer => clearTimeout(timer));
    this.retryTimers = [];
  }

  onLiveProcessingStarted(subscription: ExtendedCatchUpSubscription) {
    subscription.isLive = true;
    this.logger.log('Live processing of EventStore events started!');
  }

  addEventHandlers(eventHandlers: IEventConstructors) {
    this.eventHandlers = { ...this.eventHandlers, ...eventHandlers };
  }
}
