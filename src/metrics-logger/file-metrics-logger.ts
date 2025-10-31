import { MiniCloudMetrics } from './mini-cloud-metrics';
import * as winston from 'winston';
import 'winston-daily-rotate-file';
import { Metrics, MetricsFactory } from '@ultrasa/dev-kit';
const { MESSAGE } = require('triple-beam');

const simpleJsonFormatBuilder = winston.format((info) => {
  info[MESSAGE] = info.message;
  return info;
});

export interface Props {
  readonly maxFiles?: string;
}

export class FileMetricsFactory implements MetricsFactory {
  private readonly logger: winston.Logger;
  private readonly outputDir: string;
  private readonly maxFiles: string;

  constructor(outputDir: string, props?: Props) {
    this.outputDir = outputDir;
    this.maxFiles = props?.maxFiles ?? '3d';
    const transport = new winston.transports.DailyRotateFile({
      dirname: this.outputDir,
      utc: true,
      filename: '$%DATE%.metrics',
      level: 'info',
      datePattern: 'YYYY-MM-DD-HH',
      maxFiles: this.maxFiles,
    });

    this.logger = winston.createLogger({
      level: 'info',
      format: simpleJsonFormatBuilder(),
      transports: transport,
    });
  }

  create(namespace: string): Metrics {
    return new MiniCloudMetrics(namespace, (text) => this.logger.info(text));
  }
}
