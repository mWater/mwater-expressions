import { AsyncPriorityQueue } from "async";
import { JsonQLQuery } from "jsonql";
import DataSource from "./DataSource";
import PriorityDataSource from "./PriorityDataSource";
import { Row } from "./types";
export default class PriorityDataQueue {
    dataSource: DataSource;
    performQueryPriorityQueue: AsyncPriorityQueue<JsonQLQuery>;
    constructor(dataSource: DataSource, concurrency: number);
    createPriorityDataSource(priority: number): PriorityDataSource;
    performQuery(query: JsonQLQuery, cb: (error: any, rows: Row[]) => void, priority: number): void;
    clearCache(): void;
    getCacheExpiry(): number;
    getImageUrl(imageId: string, height?: number): string;
    kill(): void;
}
