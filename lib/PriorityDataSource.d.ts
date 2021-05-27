import { JsonQLQuery } from "jsonql";
import DataSource from "./DataSource";
import PriorityDataQueue from "./PriorityDataQueue";
import { Row } from "./types";
export default class PriorityDataSource extends DataSource {
    priorityDataQueue: PriorityDataQueue;
    priority: number;
    constructor(priorityDataQueue: PriorityDataQueue, priority: number);
    performQuery(query: JsonQLQuery): Promise<Row[]>;
    performQuery(query: JsonQLQuery, cb: (error: any, rows: Row[]) => void): void;
    getImageUrl(imageId: string, height?: number): string;
    clearCache(): void;
    getCacheExpiry(): number;
}
