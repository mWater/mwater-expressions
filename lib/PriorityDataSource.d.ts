import { JsonQLQuery } from "jsonql";
import DataSource from "./DataSource";
import PriorityDataQueue from "./PriorityDataQueue";
import { Row } from "./types";
/**
 * Behaves like a DataSource
 * Created by a PriorityDataQueue
 * Forwards performQuery call to the PriorityDataQueue that will forward them to the DataSource
 */
export default class PriorityDataSource extends DataSource {
    priorityDataQueue: PriorityDataQueue;
    priority: number;
    constructor(priorityDataQueue: PriorityDataQueue, priority: number);
    performQuery(query: JsonQLQuery): Promise<Row[]>;
    performQuery(query: JsonQLQuery, cb: (error: any, rows: Row[]) => void): void;
    getImageUrl(imageId: string, height?: number): string;
    /** Clears the cache if possible with this data source */
    clearCache(): void;
    /** Get the cache expiry time in ms from epoch. No cached items before this time will be used */
    getCacheExpiry(): number;
}
