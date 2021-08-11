import { JsonQLQuery } from "jsonql"
import DataSource from "./DataSource"
import { Row } from "./types"

/** Data source which always returns empty queries */
export default class NullDataSource extends DataSource {
  // Performs a single query. Calls cb with rows
  performQuery(query: JsonQLQuery): Promise<Row[]>
  performQuery(query: JsonQLQuery, cb: (error: any, rows: Row[]) => void): void
  performQuery(query: JsonQLQuery, cb?: (err: any, results: Row[]) => void): Promise<Row[]> | void {
    if (cb) {
      cb(null, [])
      return
    } else {
      return Promise.resolve([])
    }
  }
}
