// Copyright 2019-2023 Tauri Programme within The Commons Conservancy
// SPDX-License-Identifier: Apache-2.0
// SPDX-License-Identifier: MIT

import { invoke, Channel } from '@tauri-apps/api/core'
import { EventEmitter } from 'events'

export interface ConnectionConfig {
  /**
   * Read buffer capacity. The default value is 128 KiB.
   */
  readBufferSize?: number
  /** The target minimum size of the write buffer to reach before writing the data to the underlying stream. The default value is 128 KiB.
   *
   * If set to 0 each message will be eagerly written to the underlying stream. It is often more optimal to allow them to buffer a little, hence the default value.
   */
  writeBufferSize?: number
  /** The max size of the write buffer in bytes. Setting this can provide backpressure in the case the write buffer is filling up due to write errors. The default value is unlimited.
   *
   * Note: The write buffer only builds up past write_buffer_size when writes to the underlying stream are failing. So the write buffer can not fill up if you are not observing write errors.
   *
   * Note: Should always be at least write_buffer_size + 1 message and probably a little more depending on error handling strategy.
   */
  maxWriteBufferSize?: number
  /**
   * The maximum size of an incoming message. The string "none" means no size limit. The default value is 64 MiB which should be reasonably big for all normal use-cases but small enough to prevent memory eating by a malicious user.
   */
  maxMessageSize?: number | 'none'
  /**
   * The maximum size of a single incoming message frame. The string "none" means no size limit. The limit is for frame payload NOT including the frame header. The default value is 16 MiB which should be reasonably big for all normal use-cases but small enough to prevent memory eating by a malicious user.
   */
  maxFrameSize?: number | 'none'
  /**
   * When set to true, the server will accept and handle unmasked frames from the client. According to the RFC 6455, the server must close the connection to the client in such cases, however it seems like there are some popular libraries that are sending unmasked frames, ignoring the RFC. By default this option is set to false, i.e. according to RFC 6455.
   */
  acceptUnmaskedFrames?: boolean
  /**
   * Additional connect request headers.
   */
  headers?: HeadersInit
}

export interface MessageKind<T, D> {
  type: T
  data: D
}

export interface CloseFrame {
  code: number
  reason: string
}

export type Message =
  | MessageKind<'Text', string>
  | MessageKind<'Binary', number[]>
  | MessageKind<'Ping', number[]>
  | MessageKind<'Pong', number[]>
  | MessageKind<'Close', CloseFrame | null>
  | MessageKind<'Error', string> // Can only be received from Rust, should never be constructed

export class WebSocket {
  id: number
  private readonly listeners: Set<(arg: Message) => void>

  constructor(id: number, listeners: Set<(arg: Message) => void>) {
    this.id = id
    this.listeners = listeners
  }

  static async connect(
    url: string,
    config?: ConnectionConfig
  ): Promise<WebSocket> {
    const listeners: Set<(arg: Message) => void> = new Set()

    const onMessage = new Channel<Message>()
    onMessage.onmessage = (message: Message): void => {
      listeners.forEach((l) => {
        l(message)
      })
    }

    if (config?.headers) {
      config.headers = Array.from(new Headers(config.headers).entries())
    }

    return await invoke<number>('plugin:websocket|connect', {
      url,
      onMessage,
      config
    }).then((id) => new WebSocket(id, listeners))
  }

  addListener(cb: (arg: Message) => void): () => void {
    this.listeners.add(cb)

    return () => {
      this.listeners.delete(cb)
    }
  }

  async send(message: Message | string | number[]): Promise<void> {
    let m: Message
    if (typeof message === 'string') {
      m = { type: 'Text', data: message }
    } else if (typeof message === 'object' && 'type' in message) {
      m = message
    } else if (Array.isArray(message)) {
      m = { type: 'Binary', data: message }
    } else {
      throw new Error(
        'invalid `message` type, expected a `{ type: string, data: any }` object, a string or a numeric array'
      )
    }
    await invoke('plugin:websocket|send', {
      id: this.id,
      message: m
    })
  }

  async disconnect(): Promise<void> {
    await this.send({
      type: 'Close',
      data: {
        code: 1000,
        reason: 'Disconnected by client'
      }
    })
  }
}

export class WebSocketServer extends EventEmitter {
  id: number

  constructor(id: number) {
    super()
    this.id = id
  }

  static async start(port: number): Promise<WebSocketServer> {
    const onConnection = new Channel<number>()

    // Request a new server on the specified port
    let newServerId = await invoke<number>('plugin:websocket|new_server', {
      port,
      onConnection
    })

    let newServer = new WebSocketServer(newServerId)

    // Emit everytime a connection is spawned
    onConnection.onmessage = async (connId: number) => {
      newServer.emit('connection', WebSocketServerConnection.start(connId))
    }

    return newServer
  }

  async close(): Promise<void> {
    await invoke('plugin:websocket|stop_server', { id: this.id })
  }
}

export class WebSocketServerConnection extends EventEmitter {
  id: number

  constructor(id: number) {
    super()
    this.id = id
  }

  static async start(id: number): Promise<WebSocketServerConnection> {
    const onMessage = new Channel<Message>()
    // Subscribe to connection
    await invoke<number>('plugin:websocket|subscribe_server', { id, onMessage })

    let newConnection = new WebSocketServerConnection(id)

    onMessage.onmessage = async (response: Message) => {
      if (response.type == 'Text') {
        newConnection.emit('message', response.data)
      } else if (response.type == 'Close') {
        newConnection.emit('close')
      } else if (response.type == 'Error') {
        newConnection.emit('error', response.data)
      }
    }

    return newConnection
  }

  async send(message: Message | string | number): Promise<void> {
    let m: Message
    if (typeof message === 'string') {
      m = { type: 'Text', data: message }
    } else if (typeof message === 'object' && 'type' in message) {
      m = message
    } else if (Array.isArray(message)) {
      m = { type: 'Binary', data: message }
    } else {
      throw new Error(
        'invalid `message` type, expected a `{ type: string, data: any }` object, a string or a numeric array'
      )
    }
    await invoke('plugin:websocket|send_server_conn', {
      id: this.id,
      message: m
    })
  }
}
