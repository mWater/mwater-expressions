import DataSource from "./DataSource";
import LRU from "lru-cache";
import { JsonQLQuery } from "jsonql";
import { Row } from "./types";
/** Caching data source for mWater. Requires jQuery. require explicitly: require('mwater-expressions/lib/MWaterDataSource') */
export default class MWaterDataSource extends DataSource {
    apiUrl: string;
    client: string | null | undefined;
    cacheExpiry: number;
    options: {
        serverCaching?: boolean;
        localCaching?: boolean;
        imageApiUrl?: string;
    };
    cache: LRU<string, Row[]>;
    /**
     * @param apiUrl
     * @param options serverCaching: allows server to send cached results. default true
     * localCaching allows local MRU cache. default true
     * imageApiUrl: overrides apiUrl for images
     */
    constructor(apiUrl: string, client?: string | null, options?: {
        serverCaching?: boolean;
        localCaching?: boolean;
        imageApiUrl?: string;
    });
    performQuery(query: JsonQLQuery): Promise<Row[]>;
    performQuery(query: JsonQLQuery, cb: (error: any, rows: Row[]) => void): void;
    getCacheExpiry(): number;
    clearCache(): void;
    getImageUrl(imageId: any, height: any): string;
}
