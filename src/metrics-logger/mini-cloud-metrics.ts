import { convertDimensionsToDimensionArray, mergeDimensions, Dimension, Dimensions, Metrics } from '@ultrasa/dev-kit';
import { MetricItem } from '@ultrasa/mini-cloud-models';
import jsonStringify from 'json-stable-stringify';

export class MiniCloudMetrics implements Metrics {
  readonly namespace: string;
  private dimensions: ReadonlyArray<Dimension>;
  private properties: { [key: string]: unknown };
  private readonly report: (message: string) => void;

  constructor(namespace: string, report: (message: string) => void) {
    this.namespace = namespace;
    this.dimensions = [];
    this.properties = {};
    this.report = report;
  }

  setDimensions(dimensions: Dimensions | ReadonlyArray<Dimension> | undefined): Metrics {
    this.dimensions = convertDimensionsToDimensionArray(dimensions);
    return this;
  }

  setProperty(key: string, value: unknown): Metrics {
    this.properties[key] = value;
    return this;
  }

  setTimestamp(timestamp: Date): Metrics {
    return this;
  }

  private print(item: MetricItem): void {
    const text = jsonStringify(item);
    if (typeof text === 'string') {
      this.report(text);
    }
  }

  async flush(): Promise<void> {}
  async close(): Promise<void> {}

  time(name: string, value: number, dimensions?: Dimensions | ReadonlyArray<Dimension> | undefined): void {
    this.print({
      namespace: this.namespace,
      dimensions: mergeDimensions(this.dimensions, convertDimensionsToDimensionArray(dimensions)),
      name,
      value,
      unit: 'ms',
      timestamp: new Date().toISOString(),
    });
  }

  timer<T>(func: () => T, name: string, dimensions?: Dimensions | ReadonlyArray<Dimension> | undefined): T {
    const startTimestamp = Date.now();
    const result = func();
    this.print({
      namespace: this.namespace,
      dimensions: mergeDimensions(this.dimensions, convertDimensionsToDimensionArray(dimensions)),
      name,
      value: Date.now() - startTimestamp,
      unit: 'ms',
      timestamp: new Date().toISOString(),
    });
    return result;
  }

  async asyncTimer<T>(func: () => Promise<T>, name: string, dimensions?: Dimensions | ReadonlyArray<Dimension> | undefined): Promise<T> {
    const startTimestamp = Date.now();
    const result = await func();
    this.print({
      namespace: this.namespace,
      dimensions: mergeDimensions(this.dimensions, convertDimensionsToDimensionArray(dimensions)),
      name,
      value: Date.now() - startTimestamp,
      unit: 'ms',
      timestamp: new Date().toISOString(),
    });
    return result;
  }

  count(name: string, value: number, dimensions?: Dimensions | ReadonlyArray<Dimension> | undefined): void {
    this.print({
      namespace: this.namespace,
      dimensions: mergeDimensions(this.dimensions, convertDimensionsToDimensionArray(dimensions)),
      name,
      value,
      unit: 'count',
      timestamp: new Date().toISOString(),
    });
  }

  incrementCounter(name: string, dimensions?: Dimensions | ReadonlyArray<Dimension> | undefined): void {
    this.print({
      namespace: this.namespace,
      dimensions: mergeDimensions(this.dimensions, convertDimensionsToDimensionArray(dimensions)),
      name,
      value: 1,
      unit: 'count',
      timestamp: new Date().toISOString(),
    });
  }

  async asyncCall<T>(func: () => Promise<T>, name: string, dimensions?: Dimensions | ReadonlyArray<Dimension> | undefined): Promise<T> {
    const startTimestamp = Date.now();
    const mergedDimensions = mergeDimensions(this.dimensions, convertDimensionsToDimensionArray(dimensions));

    this.print({
      namespace: this.namespace,
      dimensions: mergedDimensions,
      name: `${name}.Count`,
      value: 1,
      unit: 'count',
      timestamp: new Date().toISOString(),
    });

    try {
      const result = await func();
      this.print({
        namespace: this.namespace,
        dimensions: mergedDimensions,
        name: `${name}.Error`,
        value: 0,
        unit: 'count',
        timestamp: new Date().toISOString(),
      });
      return result;
    } catch (err) {
      this.print({
        namespace: this.namespace,
        dimensions: mergedDimensions,
        name: `${name}.Error`,
        value: 1,
        unit: 'count',
        timestamp: new Date().toISOString(),
      });
      throw err;
    } finally {
      this.print({
        namespace: this.namespace,
        dimensions: mergedDimensions,
        name: `${name}.Latency`,
        value: Date.now() - startTimestamp,
        unit: 'ms',
        timestamp: new Date().toISOString(),
      });
    }
  }

  number(name: string, value: number, dimensions?: Dimensions | ReadonlyArray<Dimension> | undefined): void {
    this.print({
      namespace: this.namespace,
      dimensions: mergeDimensions(this.dimensions, convertDimensionsToDimensionArray(dimensions)),
      name,
      value: value,
      unit: 'unitless',
      timestamp: new Date().toISOString(),
    });
  }

  percent(name: string, value: number, dimensions?: Dimensions | ReadonlyArray<Dimension> | undefined): void {
    this.print({
      namespace: this.namespace,
      dimensions: mergeDimensions(this.dimensions, convertDimensionsToDimensionArray(dimensions)),
      name,
      value: Math.round(value * 1000) / 1000,
      unit: 'percent',
      timestamp: new Date().toISOString(),
    });
  }
}
