import _ from "lodash"
import { Expr, Variable, CaseExpr, ScalarExpr, VariableExpr, ScoreExpr, BuildEnumsetExpr } from "./types"
import Schema from "./Schema"
import ExprUtils from "./ExprUtils"
import moment from "moment"
import { getExprExtension } from "./extensions"

/** Represents a row to be evaluated */
export interface PromiseExprEvaluatorRow {
  /** gets primary key of row */
  getPrimaryKey(): Promise<any>

  /** gets the value of a column. 
   * For joins, getField will get either the primary key or array of primary keys
   */
  getField(columnId: string): Promise<any>

  /** Get array of rows for 1-n and n-n joins and a row or null for n-1 and 1-1 joins */
  followJoin(columnId: string): Promise<null | PromiseExprEvaluatorRow | PromiseExprEvaluatorRow[]>
}

export interface PromiseExprEvaluatorContext {
  /** current row. Optional for aggr expressions */
  row?: PromiseExprEvaluatorRow

  /** array of rows (for aggregate expressions) */
  rows?: PromiseExprEvaluatorRow[]
}

/** Expression evaluator that is promise-based */
export class PromiseExprEvaluator {
  schema?: Schema
  locale?: string
  variables?: Variable[]
  variableValues?: { [variableId: string]: Expr }

  /** variableValues are the expressions which the variable contains */
  constructor(options: { 
    schema?: Schema
    locale?: string
    variables?: Variable[]
    variableValues?: { [variableId: string]: any }
  }) {
    this.schema = options.schema
    this.locale = options.locale
    this.variables = options.variables
    this.variableValues = options.variableValues
  }

  /** Evaluate an expression given the context */
  async evaluate(expr: Expr, context: PromiseExprEvaluatorContext): Promise<any> {
    if (!expr) {
      return null
    }

    switch (expr.type) {
      case "field":
        // If schema is present and column is an expression column, use that
        if (this.schema && this.schema.getColumn(expr.table, expr.column) && this.schema.getColumn(expr.table, expr.column)!.expr) {
          return await this.evaluate(this.schema.getColumn(expr.table, expr.column)!.expr!, context)
        }
        if (!context.row) {
          return null
        }

        // Get field from row
        const value = await context.row.getField(expr.column)

        return value
      case "literal":
        return expr.value
      case "op":
        return await this.evaluateOp(expr.table, expr.op, expr.exprs, context)
      case "id":
        if (!context.row) {
          return null
        }
        return context.row.getPrimaryKey()
      case "case":
        return await this.evaluateCase(expr, context)
      case "scalar":
        return await this.evaluateScalar(expr, context)
      case "score":
        return await this.evaluateScore(expr, context)
      case "build enumset":
        return await this.evaluateBuildEnumset(expr, context)
      case "variable":
        return await this.evaluateVariable(expr, context)
      case "extension":
        return await getExprExtension(expr.extension).evaluate(expr, context, this.schema, this.locale, this.variables, this.variableValues)
      default:
        throw new Error(`Unsupported expression type ${(expr as any).type}`)
    }
  }

  /** Evaluate an expression synchronously */
  evaluateSync(expr: Expr): any {
    if (!expr) {
      return null
    }

    switch (expr.type) {
      case "literal":
        return expr.value
      case "op":
        return this.evaluateOpValues(expr.op, expr.exprs, expr.exprs.map(e => this.evaluateSync(e)))
      case "case":
        // TODO
        throw new Error("Synchronous case not supported")
      case "score":
        // TODO
        throw new Error("Synchronous score not supported")
      case "build enumset":
        // TODO
        throw new Error("Synchronous build enumset not supported")
      case "variable":
        if (expr.table) {
          throw new Error(`Synchronous table variables not supported`)
        }
    
        // Get variable
        const variable = _.findWhere(this.variables || [], {
          id: expr.variableId
        })
        if (!variable) {
          throw new Error(`Variable ${expr.variableId} not found`)
        }

        // Get value
        const value = this.variableValues![variable.id]
        if (value == null) {
          return null
        }

        // Evaluate variable
        return this.evaluateSync(value)
      case "extension":
        return getExprExtension(expr.extension).evaluateSync(expr, this.schema, this.locale, this.variables, this.variableValues)
      default:
        throw new Error(`Unsupported expression type ${(expr as any).type}`)
    }
  }

  async evaluateBuildEnumset(expr: BuildEnumsetExpr, context: PromiseExprEvaluatorContext): Promise<any> {
    // Evaluate each boolean
    const result: string[] = []

    for (const key in expr.values) {
      const val = await this.evaluate(expr.values[key], context)
      if (val) {
        result.push(key)
      }
    }
    return result
  }

  async evaluateScore(expr: ScoreExpr, context: PromiseExprEvaluatorContext): Promise<any> {
    // Get input value
    const input = await this.evaluate(expr.input, context)
    if (!input) {
      return null
    }

    if (_.isArray(input)) {
      let sum = 0
      for (const inputVal of input) {
        if (expr.scores[inputVal as any]) {
          sum += await this.evaluate(expr.scores[inputVal as any], context)
        }
      }
      return sum
    }
    else if (expr.scores[input]) {
      return await this.evaluate(expr.scores[input as any], context)
    }
    else {
      return 0
    }
  }

  async evaluateCase(expr: CaseExpr, context: PromiseExprEvaluatorContext): Promise<any> {
    for (const cs of expr.cases) {
      const when = await this.evaluate(cs.when, context)
      if (when) {
        return await this.evaluate(cs.then, context)
      }
    }
    return await this.evaluate(expr.else, context)
  }

  async evaluateScalar(expr: ScalarExpr, context: PromiseExprEvaluatorContext): Promise<any> {
    if (!context.row) {
      return null
    }

    // Follow each join, either expanding into array if returns multiple, or single row if one row
    let state: PromiseExprEvaluatorRow | PromiseExprEvaluatorRow[] | null = context.row
    for (const join of expr.joins) {
      // Null or [] is null
      if (!state || (_.isArray(state) && state.length == 0)) {
        return null
      }

      if (_.isArray(state)) {
        // State is an array of rows. Follow joins and flatten to rows
        const temp: any = await Promise.all(state.map((st: PromiseExprEvaluatorRow) => st.followJoin(join)))
        state = _.compact(_.flattenDeep(temp))
      }
      else {
        // State is a single row. Follow
        state = await state.followJoin(join)
      }
    }

    // Evaluate expression on new context
    if (_.isArray(state)) {
      return await this.evaluate(expr.expr, { rows: state })
    }
    else {
      return await this.evaluate(expr.expr, { row: state || undefined })
    }
  }

  async evaluateOp(table: string | undefined, op: string, exprs: Expr[], context: PromiseExprEvaluatorContext) {
    // If aggregate op
    if (ExprUtils.isOpAggr(op)) {
      return this.evaluteAggrOp(table!, op, exprs, context)
    }

    // is latest is special case for window-like function
    if (op == "is latest") {
      return await this.evaluateIsLatest(table!, exprs, context)
    }
  
    // Evaluate exprs
    const values = await Promise.all(exprs.map(expr => this.evaluate(expr, context)))
    return this.evaluateOpValues(op, exprs, values)
  }

  /** NOTE: This is not technically correct. It's not a window function (as window
   * functions can't be used in where clauses) but rather a special query */
  async evaluateIsLatest(table: string, exprs: Expr[], context: PromiseExprEvaluatorContext) {
    // Fail quietly if no ordering or no schema
    if (!this.schema || !this.schema.getTable(table) || !this.schema.getTable(table)!.ordering) {
      console.warn("evaluateIsLatest does not work without schema and ordering")
      return false
    }
  
    // Fail quietly if no rows
    if (!context.rows) {
      console.warn("evaluateIsLatest does not work without rows context")
      return false
    }

    // Null if no row
    if (!context.row) {
      return null
    }
  
    // Evaluate lhs (value to group by) for all rows
    const lhss = await Promise.all(context.rows.map(row => this.evaluate(exprs[0], { row: row })))

    // Evaluate pk for all rows
    const pks = await Promise.all(context.rows.map(row => row.getPrimaryKey()))

    // Evaluate all rows by ordering
    const orderValues = await Promise.all(context.rows.map(row => row.getField(this.schema!.getTable(table)!.ordering!)))

    // Evaluate filter value for all rows if present
    const filters = await Promise.all(context.rows.map(row => this.evaluate(exprs[0], { row: row })))

    let items = _.map(lhss, (lhs, index) => ({ lhs: lhs, pk: pks[index], ordering: orderValues[index], filter: filters[index] }))
  
    // Filter
    if (exprs[1]) {
      items = _.filter(items, item => item.filter)
    }

    // Group by lhs
    const groups = _.groupBy(items, "lhs")
  
    // Keep latest of each group
    let latests = []
    for (const lhs in groups) {
      const items = groups[lhs]
      latests.push(_.max(items, "ordering"))
    }

    // Get pk of row
    const pk = await context.row.getPrimaryKey()
    
    // See if match
    return _.contains(_.pluck(latests, "pk"), pk)
  }
  
  async evaluteAggrOp(table: string, op: string, exprs: Expr[], context: PromiseExprEvaluatorContext) {
    if (!context.rows) {
      return null
    }

    let values, orderValues, wheres, zipped, sum, ofs, count, items, value

    switch (op) {
      case "count":
        return context.rows.length
      case "sum":
        // Evaluate all rows
        values = await Promise.all(context.rows.map(row => this.evaluate(exprs[0], { row })))
        return _.sum(values)
      case "avg":
        // Evaluate all rows
        values = await Promise.all(context.rows.map(row => this.evaluate(exprs[0], { row })))
        return _.sum(values) / values.length

      // TODO. Uses window functions, so returning 100 for now
      case "percent":
        return 100
      case "min":
        // Evaluate all rows
        values = await Promise.all(context.rows.map(row => this.evaluate(exprs[0], { row })))
        return _.min(values)
      case "max":
        // Evaluate all rows
        values = await Promise.all(context.rows.map(row => this.evaluate(exprs[0], { row })))
        return _.max(values)
      case "last":
        // Fail quietly if no ordering or no schema
        if (!this.schema || !this.schema.getTable(table) || !this.schema.getTable(table)!.ordering) {
          console.warn("last does not work without schema and ordering");
          return null
        }
        // Evaluate all rows by ordering
        orderValues = await Promise.all(context.rows.map(row => row.getField(this.schema!.getTable(table)!.ordering!)))

        // Evaluate all rows
        values = await Promise.all(context.rows.map(row => this.evaluate(exprs[0], { row })))

        zipped = _.zip(values, orderValues)

        // Sort by ordering reverse
        zipped = _.sortByOrder(zipped, [entry => entry[1]], ["desc"])
        values = _.map(zipped, entry => entry[0])
        
        // Take first non-null
        for (let i = 0 ; i < values.length ; i++) {
          if (values[i] != null) {
            return values[i]
          }
        }
        return null
      case "last where":
        // Fail quietly if no ordering or no schema
        if (!this.schema || !this.schema.getTable(table) || !this.schema.getTable(table)!.ordering) {
          console.warn("last where does not work without schema and ordering");
          return null
        }
        // Evaluate all rows by ordering
        orderValues = await Promise.all(context.rows.map(row => row.getField(this.schema!.getTable(table)!.ordering!)))

        // Evaluate all rows
        values = await Promise.all(context.rows.map(row => this.evaluate(exprs[0], { row })))

        // Evaluate all rows by where
        wheres = await Promise.all(context.rows.map(row => this.evaluate(exprs[1], { row })))

        // Find largest
        if (orderValues.length == 0)
          return null

        let index = -1
        let largest: any = null
        for (let i = 0 ; i < context.rows.length ; i++) {
          if ((wheres[i] || !exprs[1]) && (index == -1 || orderValues[i] > largest) && values[i] != null) {
            index = i
            largest = orderValues[i]
          }
        }

        if (index >= 0) {
          return values[index]
        }
        else {
          return null
        }
      case "previous":
        // Fail quietly if no ordering or no schema
        if (!this.schema || !this.schema.getTable(table) || !this.schema.getTable(table)!.ordering) {
          console.warn("last where does not work without schema and ordering");
          return null
        }
        // Evaluate all rows by ordering
        orderValues = await Promise.all(context.rows.map(row => row.getField(this.schema!.getTable(table)!.ordering!)))

        // Evaluate all rows
        values = await Promise.all(context.rows.map(row => this.evaluate(exprs[0], { row })))

        zipped = _.zip(values, orderValues)

        // Sort by ordering reverse
        zipped = _.sortByOrder(zipped, [entry => entry[1]], ["desc"])
        values = _.map(zipped, entry => entry[0])
        
        // Take second non-null
        values = _.filter(values, v => v != null)
        if (values[1] != null) {
          return values[1]
        }
        return null

      case "first":
        // Fail quietly if no ordering or no schema
        if (!this.schema || !this.schema.getTable(table) || !this.schema.getTable(table)!.ordering) {
          console.warn("first does not work without schema and ordering");
          return null
        }
        // Evaluate all rows by ordering
        orderValues = await Promise.all(context.rows.map(row => row.getField(this.schema!.getTable(table)!.ordering!)))

        // Evaluate all rows
        values = await Promise.all(context.rows.map(row => this.evaluate(exprs[0], { row })))

        zipped = _.zip(values, orderValues)

        // Sort by ordering asc
        zipped = _.sortByOrder(zipped, [entry => entry[1]], ["asc"])
        values = _.map(zipped, entry => entry[0])
        
        // Take first non-null
        for (let i = 0 ; i < values.length ; i++) {
          if (values[i] != null) {
            return values[i]
          }
        }
        return null

      case "first where":
        // Fail quietly if no ordering or no schema
        if (!this.schema || !this.schema.getTable(table) || !this.schema.getTable(table)!.ordering) {
          console.warn("first where does not work without schema and ordering");
          return null
        }
        // Evaluate all rows by ordering
        orderValues = await Promise.all(context.rows.map(row => row.getField(this.schema!.getTable(table)!.ordering!)))

        // Evaluate all rows
        values = await Promise.all(context.rows.map(row => this.evaluate(exprs[0], { row })))

        // Evaluate all rows by where
        wheres = await Promise.all(context.rows.map(row => this.evaluate(exprs[1], { row })))

        // Find smallest
        if (orderValues.length == 0)
          return null

        index = -1
        let smallest: any = null
        for (let i = 0 ; i < context.rows.length ; i++) {
          if ((wheres[i] || !exprs[1]) && (index == -1 || orderValues[i] < smallest) && values[i] != null) {
            index = i
            smallest = orderValues[i]
          }
        }

        if (index >= 0) {
          return values[index]
        }
        else {
          return null
        }
      case "count where":
        // Evaluate all rows by where
        wheres = await Promise.all(context.rows.map(row => this.evaluate(exprs[0], { row })))
        return wheres.filter(w => w === true).length
      case "sum where":
        // Evaluate all rows
        values = await Promise.all(context.rows.map(row => this.evaluate(exprs[0], { row })))

        // Evaluate all rows by where
        wheres = await Promise.all(context.rows.map(row => this.evaluate(exprs[1], { row })))
        sum = 0
        for (let i = 0 ; i < context.rows.length ; i++) {
          if (wheres[i] === true) {
            sum += values[i]
          }
        }
        return sum
      case "percent where":
        // Evaluate all rows
        wheres = await Promise.all(context.rows.map(row => this.evaluate(exprs[0], { row })))

        // Evaluate all rows by where
        ofs = await Promise.all(context.rows.map(row => this.evaluate(exprs[1], { row })))
        sum = 0
        count = 0
        for (let i = 0 ; i < context.rows.length ; i++) {
          if (!exprs[1] || ofs[i] == true) {
            count++
            if (wheres[i] === true) {
              sum += 1
            }
          }
        }
        if (count === 0) {
          return null
        } 
        else {
          return sum / count * 100
        }
      case "min where":
        // Evaluate all rows
        values = await Promise.all(context.rows.map(row => this.evaluate(exprs[0], { row })))

        // Evaluate all rows by where
        wheres = await Promise.all(context.rows.map(row => this.evaluate(exprs[1], { row })))
        items = []
        for (let i = 0 ; i < context.rows.length ; i++) {
          if (wheres[i] === true) {
            items.push(values[i]);
          }
        }
        value = _.min(items)
        return value != null ? value : null
      case "max where":
        // Evaluate all rows
        values = await Promise.all(context.rows.map(row => this.evaluate(exprs[0], { row })))

        // Evaluate all rows by where
        wheres = await Promise.all(context.rows.map(row => this.evaluate(exprs[1], { row })))
        items = []
        for (let i = 0 ; i < context.rows.length ; i++) {
          if (wheres[i] === true) {
            items.push(values[i]);
          }
        }
        value = _.max(items)
        return value != null ? value : null
      case "count distinct":
        // Evaluate all rows
        values = await Promise.all(context.rows.map(row => this.evaluate(exprs[0], { row })))
        return _.uniq(values).length;
      case "array_agg":
        // Evaluate all rows
        values = await Promise.all(context.rows.map(row => this.evaluate(exprs[0], { row })))
        return values
      default:
        throw new Error(`Unknown op ${op}`)
    }
  }

  /** Synchronously evaluate an op when the values are already known */
  evaluateOpValues(op: string, exprs: Expr[], values: any[]) {
    let date, point, point1, point2, v0, v1

    // Check if has null argument
    const hasNull = _.any(values, v => v == null)

    switch (op) {
      case "+":
        return _.reduce(values, function(acc, value) {
          return acc + (value != null ? value : 0)
        })
      case "*":
        if (hasNull) {
          return null
        }
        return _.reduce(values, function(acc: number, value) {
          return acc * value
        })
      case "-":
        if (hasNull) {
          return null
        }
        return values[0] - values[1]
      case "/":
        if (hasNull) {
          return null
        }
        if (values[1] === 0) {
          return null
        }
        return values[0] / values[1]
      case "and":
        if (values.length === 0) {
          return null
        }
        return _.reduce(values, function(acc, value) {
          return acc && value
        })
      case "or":
        if (values.length === 0) {
          return null
        }
        return _.reduce(values, function(acc, value) {
          return acc || value
        })
      case "not":
        if (hasNull) {
          return true
        }
        return !values[0]
      case "=":
        if (hasNull) {
          return null
        }
        return values[0] === values[1]
      case "<>":
        if (hasNull) {
          return null
        }
        return values[0] !== values[1]
      case ">":
        if (hasNull) {
          return null
        }
        return values[0] > values[1]
      case ">=":
        if (hasNull) {
          return null
        }
        return values[0] >= values[1]
      case "<":
        if (hasNull) {
          return null
        }
        return values[0] < values[1]
      case "<=":
        if (hasNull) {
          return null
        }
        return values[0] <= values[1]
      case "= false":
        if (hasNull) {
          return null
        }
        return values[0] === false
      case "is null":
        return values[0] == null
      case "is not null":
        return values[0] != null
      case "~*":
        if (hasNull) {
          return null
        }
        return values[0].match(new RegExp(values[1], "i")) != null
      case "= any":
        if (hasNull) {
          return null
        }
        return _.contains(values[1], values[0])
      case "contains":
        if (hasNull) {
          return null
        }
        return _.difference(values[1], values[0]).length === 0
      case "intersects":
        if (hasNull) {
          return null
        }
        return _.intersection(values[0], values[1]).length > 0
      case "includes":
        if (hasNull) {
          return null
        }
        return _.includes(values[0], values[1])
      case "length":
        if (hasNull) {
          return 0
        }
        return values[0].length
      case "between":
        if (hasNull) {
          return null
        }
        return values[0] >= values[1] && values[0] <= values[2]
      case "round":
        if (hasNull) {
          return null
        }
        return Math.round(values[0])
      case "floor":
        if (hasNull) {
          return null
        }
        return Math.floor(values[0])
      case "ceiling":
        if (hasNull) {
          return null
        }
        return Math.ceil(values[0])
      case "least":
        let least: number | null = null
        for (const value of values) {
          if (value != null && (least == null || value < least)) {
            least = value
          }
        }
        return least
      case "greatest":
        let greatest: number | null = null
        for (const value of values) {
          if (value != null && (greatest == null || value > greatest)) {
            greatest = value
          }
        }
        return greatest
      case "days difference":
        if (hasNull) {
          return null
        }

        // Pad to datetime (to allow date/datetime comparisons)
        v0 = values[0].length == 10 ? values[0] + "T00:00:00Z" : values[0]
        v1 = values[1].length == 10 ? values[1] + "T00:00:00Z" : values[1]

        return moment(v0, moment.ISO_8601).diff(moment(v1, moment.ISO_8601)) / 24 / 3600 / 1000
      case "months difference":
        if (hasNull) {
          return null
        }

        // Pad to datetime (to allow date/datetime comparisons)
        v0 = values[0].length == 10 ? values[0] + "T00:00:00Z" : values[0]
        v1 = values[1].length == 10 ? values[1] + "T00:00:00Z" : values[1]

        return moment(v0, moment.ISO_8601).diff(moment(v1, moment.ISO_8601)) / 24 / 3600 / 1000 / 30.5
      case "years difference":
        if (hasNull) {
          return null
        }

        // Pad to datetime (to allow date/datetime comparisons)
        v0 = values[0].length == 10 ? values[0] + "T00:00:00Z" : values[0]
        v1 = values[1].length == 10 ? values[1] + "T00:00:00Z" : values[1]
        
        return moment(v0, moment.ISO_8601).diff(moment(v1, moment.ISO_8601)) / 24 / 3600 / 1000 / 365
      case "days since":
        if (hasNull) {
          return null
        }
        return moment().diff(moment(values[0], moment.ISO_8601)) / 24 / 3600 / 1000
      case "weekofmonth":
        if (hasNull) {
          return null
        }
        return (Math.floor((moment(values[0], moment.ISO_8601).date() - 1) / 7) + 1) + ""; // Make string
      case "dayofmonth":
        if (hasNull) {
          return null
        }
        return moment(values[0], moment.ISO_8601).format("DD")
      case "month":
        if (hasNull) {
          return null
        }
        return values[0].substr(5, 2)
      case "yearmonth":
        if (hasNull) {
          return null
        }
        return values[0].substr(0, 7) + "-01"
      case "yearquarter":
        if (hasNull) {
          return null
        }
        return values[0].substr(0, 4) + "-" + moment(values[0].substr(0, 10), 'YYYY-MM-DD').quarter()
      case "yearweek":
        if (hasNull) {
          return null
        }
        const isoWeek = moment(values[0].substr(0, 10), 'YYYY-MM-DD').isoWeek()
        return values[0].substr(0, 4) + "-" + (isoWeek < 10 ? "0" + isoWeek : isoWeek)
      case "weekofyear":
        if (hasNull) {
          return null
        }
        const isoWeek2 = moment(values[0].substr(0, 10), 'YYYY-MM-DD').isoWeek()
        return isoWeek2 < 10 ? "0" + isoWeek2 : isoWeek2
      case "to date":
        if (hasNull) {
          return null
        }
        return values[0].substr(0, 10)
      case "year":
        if (hasNull) {
          return null
        }
        return values[0].substr(0, 4) + "-01-01"
      case "today":
        if (hasNull) {
          return null
        }
        date = values[0]
        return moment(date, moment.ISO_8601).format("YYYY-MM-DD") === moment().format("YYYY-MM-DD")
      case "yesterday":
        if (hasNull) {
          return null
        }
        date = values[0]
        return moment(date, moment.ISO_8601).add(1, "days").format("YYYY-MM-DD") === moment().format("YYYY-MM-DD")
      case "thismonth":
        if (hasNull) {
          return null
        }
        date = values[0]
        return moment(date, moment.ISO_8601).format("YYYY-MM") === moment().format("YYYY-MM")
      case "lastmonth":
        if (hasNull) {
          return null
        }
        date = values[0]
        return moment(date, moment.ISO_8601).add(1, "months").format("YYYY-MM") === moment().format("YYYY-MM")
      case "thisyear":
        if (hasNull) {
          return null
        }
        date = values[0]
        return moment(date, moment.ISO_8601).format("YYYY") === moment().format("YYYY")
      case "lastyear":
        if (hasNull) {
          return null
        }
        date = values[0]
        return moment(date, moment.ISO_8601).add(1, "years").format("YYYY") === moment().format("YYYY")
      case "last24hours":
        if (hasNull) {
          return null
        }
        date = values[0]
        return moment(date, moment.ISO_8601).isSameOrBefore(moment()) && moment(date, moment.ISO_8601).isAfter(moment().subtract(24, "hours"))
      case "last7days":
        if (hasNull) {
          return null
        }
        date = values[0]
        return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) && moment(date, moment.ISO_8601).isAfter(moment().subtract(7, "days"))
      case "last30days":
        if (hasNull) {
          return null
        }
        date = values[0]
        return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) && moment(date, moment.ISO_8601).isAfter(moment().subtract(30, "days"))
      case "last365days":
        if (hasNull) {
          return null
        }
        date = values[0]
        return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) && moment(date, moment.ISO_8601).isAfter(moment().subtract(365, "days"))
      case "last12months":
        if (hasNull) {
          return null
        }
        date = values[0]
        return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) && moment(date, moment.ISO_8601).isAfter(moment().subtract(11, "months").startOf('month'))
      case "last6months":
        if (hasNull) {
          return null
        }
        date = values[0]
        return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) && moment(date, moment.ISO_8601).isAfter(moment().subtract(5, "months").startOf('month'))
      case "last3months":
        if (hasNull) {
          return null
        }
        date = values[0]
        return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) && moment(date, moment.ISO_8601).isAfter(moment().subtract(2, "months").startOf('month'))
      case "future":
        if (hasNull) {
          return null
        }
        date = values[0]
        return moment(date, moment.ISO_8601).isAfter(moment())
      case "notfuture":
        if (hasNull) {
          return null
        }
        date = values[0]
        return !moment(date, moment.ISO_8601).isAfter(moment())
      case "current date":
        return moment().format("YYYY-MM-DD")
      case "current datetime":
        return moment().toISOString()
      case "latitude":
        if (hasNull) {
          return null
        }
        point = values[0]
        if ((point != null ? point.type : void 0) === "Point") {
          return point.coordinates[1]
        }
        break
      case "longitude":
        if (hasNull) {
          return null
        }
        point = values[0]
        if ((point != null ? point.type : void 0) === "Point") {
          return point.coordinates[0]
        }
        break
      case "distance":
        if (hasNull) {
          return null
        }
        point1 = values[0]
        point2 = values[1]
        if ((point1 != null ? point1.type : void 0) === "Point" && (point2 != null ? point2.type : void 0) === "Point") {
          return getDistanceFromLatLngInM(point1.coordinates[1], point1.coordinates[0], point2.coordinates[1], point2.coordinates[0])
        }
        break
      case "line length":
        if (hasNull) {
          return null
        }
        if (values[0].type !== "LineString") {
          return 0
        }
        let total = 0
        const coords = values[0].coordinates
        for (let i = 0; i < coords.length - 1; i++) {
          total += getDistanceFromLatLngInM(coords[i][1], coords[i][0], coords[i + 1][1], coords[i + 1][0])
        }
        return total
      case "to text":
        if (hasNull) {
          return null
        }
        if (this.schema) {
          const exprUtils = new ExprUtils(this.schema)
          return exprUtils.stringifyExprLiteral(exprs[0], values[0], this.locale)
        } else {
          return values[0] + ""
        }
      default:
        throw new Error(`Unknown op ${op}`)
    }
  }

  async evaluateVariable(expr: VariableExpr, context: PromiseExprEvaluatorContext) {
    // Get variable
    const variable = _.findWhere(this.variables || [], {
      id: expr.variableId
    })
    if (!variable) {
      throw new Error(`Variable ${expr.variableId} not found`)
    }

    // Get value
    const value = this.variableValues![variable.id]
    if (value == null) {
      return null
    }

    // Evaluate
    return await this.evaluate(value, context)
  }
}

function getDistanceFromLatLngInM(lat1: number, lng1: number, lat2: number, lng2: number) {
  var R, a, c, d, dLat, dLng
  R = 6370986; // Radius of the earth in m
  dLat = deg2rad(lat2 - lat1); // deg2rad below
  dLng = deg2rad(lng2 - lng1)
  a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.sin(dLng / 2) * Math.sin(dLng / 2)
  c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
  d = R * c; // Distance in m
  return d
}

function deg2rad(deg: number) {
  return deg * (Math.PI / 180)
}
