import { JsonQLQuery } from "jsonql";
import PriorityDataQueue from "./PriorityDataQueue";
import { Row } from "./types";
export default class PriorityDataSource {
    priorityDataQueue: PriorityDataQueue;
    priority: number;
    constructor(priorityDataQueue: PriorityDataQueue, priority: number);
    performQuery(query: JsonQLQuery, cb: (err: any, results: Row[]) => void): void;
    getImageUrl(imageId: string, height?: number): string;
    clearCache(): void;
    getCacheExpiry(): number;
}
