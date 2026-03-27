declare module "zongji" {
  export default class ZongJi {
    constructor(connection: Record<string, unknown>);
    on(eventName: string, handler: (...args: any[]) => void): this;
    start(options?: Record<string, unknown>): void;
    stop(): void;
  }
}

declare module "@powersync/mysql-zongji" {
  export default class ZongJi {
    constructor(connection: Record<string, unknown>);
    on(eventName: string, handler: (...args: any[]) => void): this;
    start(options?: Record<string, unknown>): void;
    stop(): void;
  }
}