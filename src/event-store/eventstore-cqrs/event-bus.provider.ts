import { Injectable, OnModuleDestroy, Type } from '@nestjs/common';
import { ModuleRef } from '@nestjs/core';
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper';
import { Observable, Subscription } from 'rxjs';
import { filter } from 'rxjs/operators';
import {
  IEventHandler,
  IEvent,
  ObservableBus,
  CommandBus,
  InvalidSagaException,
  ISaga,
} from '@nestjs/cqrs';
import { EventStoreBus, IEventConstructors } from './event-store.bus';
import { EventStore } from '../event-store.class';

/**
 * Metadata keys used by @nestjs/cqrs.
 *
 * They are declared locally instead of being imported from
 * `@nestjs/cqrs/dist/decorators/constants` because deep imports into
 * `@nestjs/cqrs/dist/**` are blocked by the `exports` map introduced in
 * @nestjs/cqrs v12 (ERR_PACKAGE_PATH_NOT_EXPORTED).
 *
 * The values are part of the decorator contract and have been stable since
 * v7, so re-declaring them is safe across v7 -> v12.
 */
const SAGA_METADATA = '__saga__';
const EVENTS_HANDLER_METADATA = '__eventsHandler__';

export enum EventStoreSubscriptionType {
  Persistent,
  CatchUp,
}

export type EventStorePersistentSubscription = {
  type: EventStoreSubscriptionType.Persistent;
  stream: string;
  persistentSubscriptionName: string;
};

export type EventStoreCatchupSubscription = {
  type: EventStoreSubscriptionType.CatchUp;
  stream: string;
};

export type EventStoreSubscriptionConfig = {
  persistentSubscriptionName: string;
};

export type EventStoreSubscription =
  | EventStorePersistentSubscription
  | EventStoreCatchupSubscription;

export type EventStoreBusConfig = {
  subscriptions: EventStoreSubscription[];
  eventInstantiators: IEventConstructors;
};

/**
 * @deprecated Handlers are discovered as `InstanceWrapper`s since
 * @nestjs/cqrs v11. Retained only for backwards compatible type imports.
 */
export type EventHandlerType = Type<IEventHandler<IEvent>>;

@Injectable()
export class EventBusProvider extends ObservableBus<IEvent>
  implements OnModuleDestroy {
  private _publisher: EventStoreBus;
  private readonly subscriptions: Subscription[];

  constructor(
    private readonly commandBus: CommandBus,
    private readonly moduleRef: ModuleRef,
    private readonly eventStore: EventStore,
    private config: EventStoreBusConfig,
  ) {
    super();
    this.subscriptions = [];
    this.useDefaultPublisher();
  }

  get publisher(): EventStoreBus {
    return this._publisher;
  }

  set publisher(_publisher: EventStoreBus) {
    this._publisher = _publisher;
  }

  onModuleDestroy() {
    this.subscriptions.forEach(subscription => subscription.unsubscribe());
    this.subscriptions.length = 0;
    if (this._publisher && typeof this._publisher.dispose === 'function') {
      this._publisher.dispose();
    }
  }

  publish<T extends IEvent>(event: T, stream: string) {
    return this._publisher.publish(event, stream);
  }

  publishWithMaxAge<T extends IEvent>(event: T, stream: string) {
    return this._publisher.publishWithMaxAge(event, stream);
  }

  publishAll(events: IEvent[]) {
    return (events || []).forEach(event => this._publisher.publish(event));
  }

  bind(handler: IEventHandler<IEvent>, name: string) {
    const stream$ = name ? this.ofEventName(name) : this.subject$;
    const subscription = stream$.subscribe(event => handler.handle(event));
    this.subscriptions.push(subscription);
  }

  /**
   * @nestjs/cqrs v11 changed the discovery contract: `ExplorerService.explore()`
   * returns `InstanceWrapper[]` (v10 and below returned `Type[]`).
   */
  registerSagas(wrappers: InstanceWrapper<any>[] = []) {
    const sagas = wrappers
      .map((wrapper) => {
        const target = this.resolveType(wrapper);
        const metadata: string[] =
          Reflect.getMetadata(SAGA_METADATA, target) || [];
        const instance =
          wrapper.instance ?? this.moduleRef.get(target, { strict: false });
        if (!instance) {
          throw new InvalidSagaException();
        }
        return metadata.map((key: string) => instance[key].bind(instance));
      })
      .reduce((a, b) => a.concat(b), []);

    sagas.forEach(saga => this.registerSaga(saga));
  }

  register(handlers: InstanceWrapper<IEventHandler>[] = []) {
    handlers.forEach(handler => this.registerHandler(handler));
  }

  protected registerHandler(handler: InstanceWrapper<IEventHandler>) {
    const target = this.resolveType(handler);
    const instance =
      handler.instance ?? this.moduleRef.get(target, { strict: false });
    if (!instance) {
      return;
    }
    const eventsNames = this.reflectEventsNames(handler);
    eventsNames.map(event =>
      this.bind(instance as IEventHandler<IEvent>, event.name),
    );
  }

  protected ofEventName(name: string) {
    return this.subject$.pipe(
      filter(event => this.getEventName(event) === name),
    );
  }

  private getEventName(event): string {
    const { constructor } = Object.getPrototypeOf(event);
    return constructor.name as string;
  }

  protected registerSaga(saga: ISaga) {
    if (typeof saga !== 'function') {
      throw new InvalidSagaException();
    }
    const stream$ = saga(this);
    if (!(stream$ instanceof Observable)) {
      throw new InvalidSagaException();
    }

    const subscription = stream$
      .pipe(filter(e => !!e))
      .subscribe(command => this.commandBus.execute(command));

    this.subscriptions.push(subscription);
  }

  private reflectEventsNames(
    handler: InstanceWrapper<IEventHandler>,
  ): FunctionConstructor[] {
    return (
      Reflect.getMetadata(EVENTS_HANDLER_METADATA, this.resolveType(handler)) ||
      []
    );
  }

  /**
   * Resolves the class reference behind an `InstanceWrapper`. Providers created
   * through `useValue` / `useFactory` have no `metatype`, so fall back to the
   * constructor of the resolved instance.
   */
  private resolveType(wrapper: InstanceWrapper<any>): Type<any> {
    const target = wrapper.metatype ?? wrapper.instance?.constructor;
    if (!target) {
      throw new InvalidSagaException();
    }
    return target as Type<any>;
  }

  private useDefaultPublisher() {
    const pubSub = new EventStoreBus(
      this.eventStore,
      this.subject$,
      this.config,
    );
    this._publisher = pubSub;
  }
}
