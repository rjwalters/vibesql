import { EventEmitter } from 'events';
import {
  ConnectionState,
  VibeQLClientConfig,
  ProtocolFrame,
  MessageType,
} from './types.js';
import { ProtocolCodec } from './protocol.js';

/**
 * Handles TCP/WebSocket connection to VibeSQL server
 */
export class Connection extends EventEmitter {
  private config: VibeQLClientConfig;
  private state: ConnectionState = ConnectionState.DISCONNECTED;
  private socket: any = null;
  private reconnectAttempts = 0;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private receiveBuffer = Buffer.alloc(0);

  constructor(config: VibeQLClientConfig) {
    super();
    this.config = {
      host: 'localhost',
      port: 5432,
      reconnectInterval: 1000,
      maxReconnectAttempts: 5,
      queryTimeout: 30000,
      ...config,
    };
  }

  /**
   * Establish connection to server
   */
  async connect(): Promise<void> {
    if (this.state === ConnectionState.CONNECTED) {
      return;
    }

    this.setState(ConnectionState.CONNECTING);

    try {
      // Dynamic import for Node.js environment
      const net = await import('net');

      this.socket = net.createConnection({
        host: this.config.host,
        port: this.config.port,
      });

      this.socket.on('connect', () => {
        this.reconnectAttempts = 0;
        this.setState(ConnectionState.CONNECTED);
        this.emit('connect');
      });

      this.socket.on('data', (data: Buffer) => {
        this.handleData(data);
      });

      this.socket.on('error', (error: Error) => {
        this.setState(ConnectionState.ERROR);
        this.emit('error', error);
        this.attemptReconnect();
      });

      this.socket.on('close', () => {
        this.setState(ConnectionState.DISCONNECTED);
        this.emit('close');
        this.attemptReconnect();
      });

      // Wait for connection with timeout
      return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error('Connection timeout'));
          this.disconnect();
        }, this.config.queryTimeout);

        const onConnect = () => {
          clearTimeout(timeout);
          this.socket.removeListener('error', onError);
          resolve();
        };

        const onError = (error: Error) => {
          clearTimeout(timeout);
          this.socket.removeListener('connect', onConnect);
          reject(error);
        };

        this.socket.once('connect', onConnect);
        this.socket.once('error', onError);
      });
    } catch (error) {
      this.setState(ConnectionState.ERROR);
      throw error;
    }
  }

  /**
   * Disconnect from server
   */
  async disconnect(): Promise<void> {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }

    this.setState(ConnectionState.DISCONNECTED);
  }

  /**
   * Send a message to server
   */
  send(frame: Buffer): void {
    if (this.state !== ConnectionState.CONNECTED || !this.socket) {
      throw new Error('Not connected to server');
    }

    this.socket.write(frame);
  }

  /**
   * Get current connection state
   */
  getState(): ConnectionState {
    return this.state;
  }

  /**
   * Check if connected
   */
  isConnected(): boolean {
    return this.state === ConnectionState.CONNECTED;
  }

  /**
   * Handle incoming data
   */
  private handleData(data: Buffer): void {
    this.receiveBuffer = Buffer.concat([this.receiveBuffer, data]);

    // Process all complete frames in buffer
    while (true) {
      const frame = ProtocolCodec.decodeFrameHeader(this.receiveBuffer);
      if (!frame) {
        break;
      }

      // Calculate total frame size
      const frameSize = 5 + frame.payload.length;
      this.receiveBuffer = this.receiveBuffer.subarray(frameSize);

      this.emit('frame', frame);
    }
  }

  /**
   * Attempt to reconnect
   */
  private attemptReconnect(): void {
    if (this.config.maxReconnectAttempts === 0) {
      return;
    }

    if (this.reconnectAttempts >= (this.config.maxReconnectAttempts || 5)) {
      this.emit('maxReconnectAttemptsReached');
      return;
    }

    this.reconnectAttempts++;
    this.setState(ConnectionState.RECONNECTING);

    const delay = this.config.reconnectInterval! * Math.pow(2, this.reconnectAttempts - 1);

    this.reconnectTimer = setTimeout(() => {
      this.connect().catch(() => {
        // Error already handled in connect()
      });
    }, delay);
  }

  /**
   * Set connection state and emit event
   */
  private setState(newState: ConnectionState): void {
    if (this.state !== newState) {
      this.state = newState;
      this.emit('stateChange', newState);
    }
  }
}
