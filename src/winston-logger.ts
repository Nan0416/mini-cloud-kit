import { Logger, LoggerOptions, LogLevel, LoggerBuilder, compareLogLevel } from '@ultrasa/dev-kit';
import * as winston from 'winston';
import 'winston-daily-rotate-file';

export interface WinstonLoggerFactoryProps {
  readonly prefix: string;
  readonly type: string;
  readonly outputDir: string;
  readonly maxFiles?: string; // default 30d
  readonly defaultLevel?: LogLevel; // info
}

export interface WinstonLoggerOptions {
  readonly level: LogLevel;
}

export class WinstonLoggerBuilder implements LoggerBuilder {
  private readonly format: any;
  private readonly transport: any;
  private readonly nameToOptions: Record<string, WinstonLoggerOptions | undefined>;
  private readonly defaultOptions: WinstonLoggerOptions;
  private readonly logger: winston.Logger;

  constructor(props?: WinstonLoggerFactoryProps, nameToOptions?: Record<string, WinstonLoggerOptions | undefined>) {
    this.transport = new winston.transports.Console();
    this.format = winston.format.combine(winston.format.timestamp(), winston.format.json());
    this.defaultOptions = {
      level: props?.defaultLevel ?? 'info',
    };

    if (props !== undefined) {
      this.transport = new winston.transports.DailyRotateFile({
        dirname: props.outputDir,
        utc: true,
        filename: `${props.prefix}-%DATE%.${props.type}.log`,
        datePattern: 'YYYY-MM-DD-HH',
        maxFiles: typeof props.maxFiles === 'string' ? props.maxFiles : '30d', // keep for 8 days
      });
    }

    this.nameToOptions = nameToOptions ? nameToOptions : {};

    this.logger = winston.createLogger({
      level: 'silly', // the lowest level in winston, { error: 0, warn: 1, info: 2, verbose: 3, debug: 4, silly: 5 }
      format: this.format,
      transports: this.transport,
    });
  }

  build(name: string): Logger {
    const options = this.nameToOptions[name];
    return new WinstonLogger(this.logger, {
      level: options ? options.level : this.defaultOptions.level,
      name: name,
    });
  }
}

export class WinstonLogger implements Logger {
  private readonly logger: winston.Logger;

  readonly level: LogLevel;
  readonly name: string;

  constructor(logger: winston.Logger, options: LoggerOptions) {
    this.logger = logger;
    this.name = options.name;
    this.level = options.level;
  }

  private buildMetadata(meta?: any): any {
    if (typeof meta === 'object') {
      if (meta instanceof Error) {
        return {
          name: this.name,
          errorStack: meta.stack,
        };
      } else {
        return {
          name: this.name,
          ...meta,
        };
      }
    } else if (meta !== undefined || meta !== null) {
      // primitive.
      return {
        name: this.name,
        meta: meta,
      };
    } else {
      return {
        name: this.name,
      };
    }
  }

  fatal(message: string, meta?: any): Logger {
    if (compareLogLevel('fatal', this.level) <= 0) {
      this.logger.error(message, this.buildMetadata(meta));
    }
    return this;
  }

  error(message: string, meta?: any): Logger {
    if (compareLogLevel('error', this.level) <= 0) {
      this.logger.error(message, this.buildMetadata(meta));
    }
    return this;
  }

  warn(message: string, meta?: any): Logger {
    if (compareLogLevel('warn', this.level) <= 0) {
      this.logger.warn(message, this.buildMetadata(meta));
    }
    return this;
  }

  info(message: string, meta?: any): Logger {
    if (compareLogLevel('info', this.level) <= 0) {
      this.logger.info(message, this.buildMetadata(meta));
    }
    return this;
  }

  debug(message: string, meta?: any): Logger {
    if (compareLogLevel('debug', this.level) <= 0) {
      this.logger.debug(message, this.buildMetadata(meta));
    }
    return this;
  }

  trace(message: string, meta?: any): Logger {
    if (compareLogLevel('trace', this.level) <= 0) {
      this.logger.silly(message, this.buildMetadata(meta));
    }
    return this;
  }
}