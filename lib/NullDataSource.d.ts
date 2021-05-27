import { JsonQLQuery } from "jsonql";
import DataSource from "./DataSource";
import { Row } from "./types";
/** Data source which always returns empty queries */
export default class NullDataSource extends DataSource {
    performQuery(query: JsonQLQuery): Promise<Row[]>;
    performQuery(query: JsonQLQuery, cb: (error: any, rows: Row[]) => void): void;
}
