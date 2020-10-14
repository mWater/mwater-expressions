import { JsonQLQuery } from "jsonql";
import DataSource from "./DataSource";
import { Row } from "./types";
/** Data source which always returns empty queries */
export default class NullDataSource extends DataSource {
    performQuery(query: JsonQLQuery, cb: (err: any, results: Row[]) => void): void;
}
