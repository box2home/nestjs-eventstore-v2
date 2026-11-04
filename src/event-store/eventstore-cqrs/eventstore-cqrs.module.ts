import { EventBus, CommandBus, QueryBus } from '@nestjs/cqrs';
import { Global, Module, DynamicModule } from '@nestjs/common';
import { DiscoveryModule, DiscoveryService, ModuleRef } from '@nestjs/core';
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper';
import { EventBusProvider, EventStoreBusConfig } from './event-bus.provider';
import { EventStore } from '../event-store.class';
import {
  EventStoreModule,
  EventStoreModuleAsyncOptions,
} from '../event-store.module';
import { EventPublisher } from './event-publisher';

/**
 * @nestjs/cqrs metadata keys, declared locally to avoid deep imports into
 * `@nestjs/cqrs/dist/**` which are blocked by the exports map added in v12.
 */
const COMMAND_HANDLER_METADATA = '__commandHandler__';
const QUERY_HANDLER_METADATA = '__queryHandler__';
const EVENTS_HANDLER_METADATA = '__eventsHandler__';
const SAGA_METADATA = '__saga__';

@Global()
@Module({})
export class EventStoreCqrsModule {
  constructor(
    private readonly discoveryService: DiscoveryService,
    private readonly eventsBus: EventBus,
    private readonly commandsBus: CommandBus,
    private readonly queryBus: QueryBus,
  ) {}

  onModuleInit() {
    const providers = this.discoveryService.getProviders();

    const explore = (metadataKey: string): InstanceWrapper[] =>
      providers.filter((wrapper) => {
        const target = wrapper.metatype ?? wrapper.instance?.constructor;
        return !!target && !!Reflect.getMetadata(metadataKey, target);
      });

    this.eventsBus.register(explore(EVENTS_HANDLER_METADATA));
    this.commandsBus.register(explore(COMMAND_HANDLER_METADATA));
    this.queryBus.register(explore(QUERY_HANDLER_METADATA));
    this.eventsBus.registerSagas(explore(SAGA_METADATA));
  }

  static forRootAsync(
    options: EventStoreModuleAsyncOptions,
    eventStoreBusConfig: EventStoreBusConfig,
  ): DynamicModule {
    return {
      module: EventStoreCqrsModule,
      imports: [DiscoveryModule, EventStoreModule.forRootAsync(options)],
      providers: [
        CommandBus,
        QueryBus,
        EventPublisher,
        {
          provide: EventBus,
          useFactory: (commandBus, moduleRef, eventStore) => {
            return new EventBusProvider(
              commandBus,
              moduleRef,
              eventStore,
              eventStoreBusConfig,
            );
          },
          inject: [CommandBus, ModuleRef, EventStore],
        },
        {
          provide: EventBusProvider,
          useExisting: EventBus,
        },
      ],
      exports: [
        EventStoreModule,
        EventBusProvider,
        EventBus,
        CommandBus,
        QueryBus,
        EventPublisher,
      ],
    };
  }
}
