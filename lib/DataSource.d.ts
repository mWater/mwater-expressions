import { JsonQLQuery } from "jsonql";
import { Row } from "./types";
export default class DataSource {
    /** Performs a single query. Calls cb with (error, rows) or uses promise if no callback */
    performQuery(query: JsonQLQuery): Promise<Row[]>;
    performQuery(query: JsonQLQuery, cb: (error: any, rows: Row[]) => void): void;
    /** Get the url to download an image (by id from an image or imagelist column)
      Height, if specified, is minimum height needed. May return larger image
    */
    getImageUrl(imageId: string, height?: number): string;
    /** Get the url to upload an image (by id from an image or imagelist column)
      POST to upload
    */
    getImageUploadUrl(imageId: string): string;
    clearCache(): void;
    getCacheExpiry(): number;
}
