import { createConnection } from 'node-eventstore-client';
import type {
  EventStoreNodeConnection,
  ConnectionSettings,
  TcpEndPoint,
} from 'node-eventstore-client';
import { Logger, OnApplicationShutdown } from '@nestjs/common';

const INITIAL_RECONNECT_DELAY_MS = 1_000;
const MAX_RECONNECT_DELAY_MS = 30_000;

export class EventStore implements OnApplicationShutdown {
  connection: EventStoreNodeConnection;

  isConnected: boolean = false;
  retryAttempts: number;

  private logger: Logger = new Logger(this.constructor.name);
  private closed = false;
  private connecting = false;
  private reconnectTimer?: NodeJS.Timeout;

  constructor(
    private settings: ConnectionSettings,
    private endpoint: TcpEndPoint,
  ) {
    this.retryAttempts = 0;
    // Fire and forget: connect() owns error handling and reconnect scheduling.
    void this.connect();
  }

  async connect(): Promise<void> {
    if (this.closed || this.connecting) {
      return;
    }
    this.connecting = true;

    try {
      this.disposeConnection();
      this.connection = createConnection(this.settings, this.endpoint);

      this.connection.on('connected', () => {
        this.logger.log('Connection to EventStore established!');
        this.retryAttempts = 0;
        this.isConnected = true;
      });

      // Without an 'error' listener an EventEmitter throws an uncaught
      // exception, which would crash the host process on a socket error.
      this.connection.on('error', (error: Error) => {
        this.logger.error(
          `EventStore connection error: ${error.message}`,
          error.stack,
        );
      });

      this.connection.on('closed', () => {
        this.isConnected = false;
        if (this.closed) {
          return;
        }
        this.logger.error(
          `Connection to EventStore closed! reconnecting attempt(${this.retryAttempts})...`,
        );
        this.scheduleReconnect();
      });

      await this.connection.connect();
    } catch (error) {
      const err = error as Error;
      this.isConnected = false;
      this.logger.error(
        `Could not connect to EventStore: ${err.message}`,
        err.stack,
      );
      this.scheduleReconnect();
    } finally {
      this.connecting = false;
    }
  }

  getConnection(): EventStoreNodeConnection {
    return this.connection;
  }

  close(): void {
    this.closed = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }

    const connection = this.connection;
    this.isConnected = false;
    this.connection = undefined as unknown as EventStoreNodeConnection;

    if (!connection) {
      return;
    }

    // Remove listeners before closing, otherwise the 'closed' handler would
    // schedule a reconnect for a connection we intentionally shut down.
    connection.removeAllListeners();
    connection.close();
  }

  /**
   * Closes the connection when the host application shuts down.
   *
   * Runs on `app.close()`. For process signals (SIGTERM/SIGINT) the host must
   * also call `app.enableShutdownHooks()`.
   */
  onApplicationShutdown(): void {
    this.close();
  }

  private scheduleReconnect() {
    if (this.closed) {
      return;
    }

    const delay = Math.min(
      INITIAL_RECONNECT_DELAY_MS * 2 ** this.retryAttempts,
      MAX_RECONNECT_DELAY_MS,
    );
    this.retryAttempts += 1;

    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
    }

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.connect();
    }, delay);

    // Do not keep the process alive solely because of a pending reconnect.
    if (typeof this.reconnectTimer.unref === 'function') {
      this.reconnectTimer.unref();
    }
  }

  private disposeConnection() {
    const connection = this.connection;
    if (!connection) {
      return;
    }
    connection.removeAllListeners();
    try {
      connection.close();
    } catch {
      // Already closed - nothing to do.
    }
    this.connection = undefined as unknown as EventStoreNodeConnection;
  }
}
