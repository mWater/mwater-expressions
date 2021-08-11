import { JsonQLQuery } from "jsonql";
import { Row } from "./types";
export default class DataSource {
    /** Performs a single query. Calls cb with (error, rows) or uses promise if no callback */
    performQuery(query: JsonQLQuery): Promise<Row[]>;
    performQuery(query: JsonQLQuery, cb: (error: any, rows: Row[]) => void): void;
    /** Get the url to download an image (by id from an image or imagelist column)
      Height, if specified, is minimum height needed. May return larger image
      Can be used to upload by posting to this url
    */
    getImageUrl(imageId: string, height?: number): string;
    clearCache(): void;
    getCacheExpiry(): number;
}
