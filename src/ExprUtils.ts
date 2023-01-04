import _ from "lodash"
import moment from "moment"
import { getExprExtension } from "./extensions"
import Schema from "./Schema"
import {
  AggrStatus,
  CaseExpr,
  EnumValue,
  Expr,
  FieldExpr,
  LiteralType,
  LocalizedString,
  ScalarExpr,
  Variable
} from "./types"
import { WeakCache } from "./WeakCache"

// exprAggrStatus Weak cache is global to allow validator to be created and destroyed
const exprAggrStatusWeakCache = new WeakCache()

/** opItems are a list of ops for various types */
interface OpItem {
  /** e.g. "=" */
  op: string
  /** e.g. "is" */
  name: string
  /** Optional description */
  desc?: string
  /** resulting type from op. e.g. "boolean" */
  resultType: LiteralType
  /** array of types of expressions required for arguments */
  exprTypes: LiteralType[]
  /** type of n more expressions (like "and" that takes n arguments) */
  moreExprType?: LiteralType
  /** true if name goes before LHS value */
  prefix?: boolean
  /** overrides name when displayed as prefix */
  prefixLabel?: string
  /** optional condition function on LHS expr that tests if applicable (for "within" which only applies to hierarchical tables) */
  lhsCond?: (lhs: Expr, exprUtils: ExprUtils) => boolean
  /** prefer rhs literal */
  rhsLiteral?: boolean
  /** string to put between exprs when prefix type */
  joiner?: string
  /** true if aggregating (e.g. sum) */
  aggr?: boolean
  /** for aggr = true if table must be have ordering */
  ordered?: boolean
  /** placeholder for lhs expression */
  lhsPlaceholder?: string
  /** placeholder for rhs expression */
  rhsPlaceholder?: string
}

export default class ExprUtils {
  schema: Schema
  variables: Variable[]

  constructor(schema: Schema, variables?: Variable[]) {
    this.schema = schema
    this.variables = variables || []
  }

  /**
   * Search can contain resultTypes, lhsExpr, op, aggr. lhsExpr is actual expression of lhs. resultTypes is optional array of result types
   * If search ordered is not true, excludes ordered ones
   * If prefix, only prefix
   * Results are array of opItems. */
  findMatchingOpItems(search: {
    resultTypes?: LiteralType[]
    lhsExpr?: Expr
    op?: string
    ordered?: boolean
    prefix?: boolean
    aggr?: boolean
  }): OpItem[] {
    // Narrow list if op specified
    let items: OpItem[]
    if (search.op) {
      items = groupedOpItems[search.op] || []
    } else {
      items = opItems
    }

    return _.filter(items, (opItem) => {
      if (search.resultTypes) {
        if (!search.resultTypes.includes(opItem.resultType)) {
          return false
        }
      }

      if (search.aggr != null && opItem.aggr !== search.aggr) {
        return false
      }

      if (search.ordered === false && opItem.ordered) {
        return false
      }

      if (search.prefix != null && opItem.prefix !== search.prefix) {
        return false
      }

      // Handle list of specified types
      if (search.lhsExpr) {
        const lhsType = this.getExprType(search.lhsExpr)

        if (lhsType) {
          // If no expressions allowed, return false
          if (opItem.exprTypes[0] == null && opItem.moreExprType == null) {
            return false
          }

          // If doesn't match first
          if (opItem.exprTypes[0] != null && opItem.exprTypes[0] !== lhsType) {
            return false
          }

          // If doesn't match more
          if (opItem.exprTypes[0] == null && opItem.moreExprType != null && opItem.moreExprType !== lhsType) {
            return false
          }
        }
      }

      // Check lhsCond
      if (search.lhsExpr && opItem.lhsCond && !opItem.lhsCond(search.lhsExpr, this)) {
        return false
      }

      return true
    })
  }

  /** Determine if op is aggregate */
  static isOpAggr(op: string): boolean {
    return aggrOpItems[op] || false
  }

  /** Determine if op is prefix */
  static isOpPrefix(op: string): boolean {
    return _.findWhere(opItems, { op, prefix: true }) != null
  }

  /** Follows a list of joins to determine final table */
  followJoins(startTable: string, joins: string[]): string {
    let t = startTable
    for (let j of joins) {
      const joinCol = this.schema.getColumn(t, j)!
      if (joinCol.type === "join") {
        t = joinCol.join!.toTable
      } else {
        t = joinCol.idTable!
      }
    }

    return t
  }

  /** Determines if an set of joins contains a multiple */
  isMultipleJoins(table: string, joins: string[]): boolean {
    let t = table
    for (let j of joins) {
      const joinCol = this.schema.getColumn(t, j)!
      if (joinCol.type === "join") {
        if (["1-n", "n-n"].includes(joinCol.join!.type)) {
          return true
        }

        t = joinCol.join!.toTable
      } else if (joinCol.type === "id") {
        t = joinCol.idTable!
      } else if (joinCol.type === "id[]") {
        return true
      } else {
        throw new Error(`Unsupported join type ${joinCol.type}`)
      }
    }

    return false
  }

  /** Return array of { id: <enum value>, name: <localized label of enum value> } */
  getExprEnumValues(expr: Expr): EnumValue[] | null {
    if (!expr) {
      return null
    }

    if (expr.type === "field") {
      const column = this.schema.getColumn(expr.table, expr.column)
      if (!column) {
        return null
      }

      // Prefer returning specified enumValues as expr might not cover all possibilities if it's an if/then, etc.
      if (column.enumValues) {
        return column.enumValues
      }

      return null
    }

    if (expr.type === "scalar") {
      if (expr.expr) {
        return this.getExprEnumValues(expr.expr)
      }
    }

    // "last", "last where", "previous", "first", "first where" are only ops to pass through enum values
    if (
      expr.type === "op" &&
      ["last", "last where", "previous", "first", "first where"].includes(expr.op) &&
      expr.exprs[0]
    ) {
      return this.getExprEnumValues(expr.exprs[0])
    }

    // Weeks of month has predefined values (1-5 as text)
    if (expr.type === "op" && expr.op === "weekofmonth") {
      return [
        { id: "1", name: { _base: "en", en: "1" } },
        { id: "2", name: { _base: "en", en: "2" } },
        { id: "3", name: { _base: "en", en: "3" } },
        { id: "4", name: { _base: "en", en: "4" } },
        { id: "5", name: { _base: "en", en: "5" } }
      ]
    }

    // Days of month has predefined values (01-31 as text)
    if (expr.type === "op" && expr.op === "dayofmonth") {
      return [
        { id: "01", name: { _base: "en", en: "01" } },
        { id: "02", name: { _base: "en", en: "02" } },
        { id: "03", name: { _base: "en", en: "03" } },
        { id: "04", name: { _base: "en", en: "04" } },
        { id: "05", name: { _base: "en", en: "05" } },
        { id: "06", name: { _base: "en", en: "06" } },
        { id: "07", name: { _base: "en", en: "07" } },
        { id: "08", name: { _base: "en", en: "08" } },
        { id: "09", name: { _base: "en", en: "09" } },
        { id: "10", name: { _base: "en", en: "10" } },
        { id: "11", name: { _base: "en", en: "11" } },
        { id: "12", name: { _base: "en", en: "12" } },
        { id: "13", name: { _base: "en", en: "13" } },
        { id: "14", name: { _base: "en", en: "14" } },
        { id: "15", name: { _base: "en", en: "15" } },
        { id: "16", name: { _base: "en", en: "16" } },
        { id: "17", name: { _base: "en", en: "17" } },
        { id: "18", name: { _base: "en", en: "18" } },
        { id: "19", name: { _base: "en", en: "19" } },
        { id: "20", name: { _base: "en", en: "20" } },
        { id: "21", name: { _base: "en", en: "21" } },
        { id: "22", name: { _base: "en", en: "22" } },
        { id: "23", name: { _base: "en", en: "23" } },
        { id: "24", name: { _base: "en", en: "24" } },
        { id: "25", name: { _base: "en", en: "25" } },
        { id: "26", name: { _base: "en", en: "26" } },
        { id: "27", name: { _base: "en", en: "27" } },
        { id: "28", name: { _base: "en", en: "28" } },
        { id: "29", name: { _base: "en", en: "29" } },
        { id: "30", name: { _base: "en", en: "30" } },
        { id: "31", name: { _base: "en", en: "31" } }
      ]
    }

    // Month has predefined values
    if (expr.type === "op" && expr.op === "month") {
      return [
        { id: "01", name: { _base: "en", en: "January" } },
        { id: "02", name: { _base: "en", en: "February" } },
        { id: "03", name: { _base: "en", en: "March" } },
        { id: "04", name: { _base: "en", en: "April" } },
        { id: "05", name: { _base: "en", en: "May" } },
        { id: "06", name: { _base: "en", en: "June" } },
        { id: "07", name: { _base: "en", en: "July" } },
        { id: "08", name: { _base: "en", en: "August" } },
        { id: "09", name: { _base: "en", en: "September" } },
        { id: "10", name: { _base: "en", en: "October" } },
        { id: "11", name: { _base: "en", en: "November" } },
        { id: "12", name: { _base: "en", en: "December" } }
      ]
    }

    // Week of year has predefined values (01-53 as text)
    if (expr.type === "op" && expr.op === "weekofyear") {
      return [
        { id: "01", name: { _base: "en", en: "01" } },
        { id: "02", name: { _base: "en", en: "02" } },
        { id: "03", name: { _base: "en", en: "03" } },
        { id: "04", name: { _base: "en", en: "04" } },
        { id: "05", name: { _base: "en", en: "05" } },
        { id: "06", name: { _base: "en", en: "06" } },
        { id: "07", name: { _base: "en", en: "07" } },
        { id: "08", name: { _base: "en", en: "08" } },
        { id: "09", name: { _base: "en", en: "09" } },
        { id: "10", name: { _base: "en", en: "10" } },
        { id: "11", name: { _base: "en", en: "11" } },
        { id: "12", name: { _base: "en", en: "12" } },
        { id: "13", name: { _base: "en", en: "13" } },
        { id: "14", name: { _base: "en", en: "14" } },
        { id: "15", name: { _base: "en", en: "15" } },
        { id: "16", name: { _base: "en", en: "16" } },
        { id: "17", name: { _base: "en", en: "17" } },
        { id: "18", name: { _base: "en", en: "18" } },
        { id: "19", name: { _base: "en", en: "19" } },
        { id: "20", name: { _base: "en", en: "20" } },
        { id: "21", name: { _base: "en", en: "21" } },
        { id: "22", name: { _base: "en", en: "22" } },
        { id: "23", name: { _base: "en", en: "23" } },
        { id: "24", name: { _base: "en", en: "24" } },
        { id: "25", name: { _base: "en", en: "25" } },
        { id: "26", name: { _base: "en", en: "26" } },
        { id: "27", name: { _base: "en", en: "27" } },
        { id: "28", name: { _base: "en", en: "28" } },
        { id: "29", name: { _base: "en", en: "29" } },
        { id: "30", name: { _base: "en", en: "30" } },
        { id: "31", name: { _base: "en", en: "31" } },
        { id: "32", name: { _base: "en", en: "32" } },
        { id: "33", name: { _base: "en", en: "33" } },
        { id: "34", name: { _base: "en", en: "34" } },
        { id: "35", name: { _base: "en", en: "35" } },
        { id: "36", name: { _base: "en", en: "36" } },
        { id: "37", name: { _base: "en", en: "37" } },
        { id: "38", name: { _base: "en", en: "38" } },
        { id: "39", name: { _base: "en", en: "39" } },
        { id: "40", name: { _base: "en", en: "40" } },
        { id: "41", name: { _base: "en", en: "41" } },
        { id: "42", name: { _base: "en", en: "42" } },
        { id: "43", name: { _base: "en", en: "43" } },
        { id: "44", name: { _base: "en", en: "44" } },
        { id: "45", name: { _base: "en", en: "45" } },
        { id: "46", name: { _base: "en", en: "46" } },
        { id: "47", name: { _base: "en", en: "47" } },
        { id: "48", name: { _base: "en", en: "48" } },
        { id: "49", name: { _base: "en", en: "49" } },
        { id: "50", name: { _base: "en", en: "50" } },
        { id: "51", name: { _base: "en", en: "51" } },
        { id: "52", name: { _base: "en", en: "52" } },
        { id: "53", name: { _base: "en", en: "53" } }
      ]
    }

    // Case statements search for possible values
    if (expr.type === "case") {
      let enumValues: EnumValue[] = []

      for (let cse of expr.cases) {
        enumValues = enumValues.concat(this.getExprEnumValues(cse.then) || [])
      }
      enumValues = enumValues.concat(this.getExprEnumValues(expr.else) || [])
      return _.uniq(enumValues, (ev) => ev.id)
    }

    if (expr.type === "variable") {
      return _.findWhere(this.variables, { id: expr.variableId })?.enumValues || null
    }

    if (expr.type == "extension") {
      return getExprExtension(expr.extension).getExprEnumValues(expr, this.schema, this.variables)
    }

    return null
  }

  /** Gets the id table of an expression of type id */
  getExprIdTable(expr: Expr): string | null {
    if (!expr) {
      return null
    }

    if (expr.type === "literal" && ["id", "id[]"].includes(expr.valueType)) {
      return expr.idTable!
    }

    if (expr.type === "id") {
      return expr.table
    }

    if (expr.type === "scalar") {
      return this.getExprIdTable(expr.expr)
    }

    // Handle fields
    if (expr.type === "field") {
      const column = this.schema.getColumn(expr.table, expr.column)!

      if (column?.type === "join") {
        return column.join!.toTable
      }

      if (["id", "id[]"].includes(column?.type!)) {
        return column.idTable!
      }

      return null
    }

    if (expr.type === "variable") {
      return _.findWhere(this.variables, { id: expr.variableId })?.idTable || null
    }

    if (expr.type == "extension") {
      return getExprExtension(expr.extension).getExprIdTable(expr, this.schema, this.variables)
    }

    return null
  }

  /** Gets the type of an expression */
  getExprType(expr: Expr): LiteralType | null {
    let type
    if (expr == null || !expr.type) {
      return null
    }

    switch (expr.type) {
      case "field":
        var column = this.schema.getColumn(expr.table, expr.column)
        if (column) {
          if (column.type === "join") {
            if (["1-1", "n-1"].includes(column.join!.type)) {
              return "id"
            } else {
              return "id[]"
            }
          }
          return column.type as LiteralType
        }
        return null
      case "id":
        return "id"
      case "scalar":
        // Legacy support:
        if (expr.aggr) {
          return this.getExprType({ type: "op", op: expr.aggr, table: expr.table, exprs: [expr.expr] })
        }

        return this.getExprType(expr.expr)
      case "op":
        // Check for single-type ops
        var matchingOpItems = this.findMatchingOpItems({ op: expr.op })
        var resultTypes = _.uniq(_.compact(_.pluck(matchingOpItems, "resultType")))
        if (resultTypes.length === 1) {
          return resultTypes[0]
        }

        // Get possible ops
        matchingOpItems = this.findMatchingOpItems({ op: expr.op, lhsExpr: expr.exprs[0] })

        // Get unique resultTypes
        resultTypes = _.uniq(_.compact(_.pluck(matchingOpItems, "resultType")))
        if (resultTypes.length === 1) {
          return resultTypes[0]
        }
        return null

      case "literal":
        return expr.valueType as LiteralType
      case "case":
        // Use type of first then that has value
        for (let cse of expr.cases) {
          type = this.getExprType(cse.then)
          if (type) {
            return type
          }
        }
        return this.getExprType(expr.else)
      case "build enumset":
        return "enumset"
      case "score":
        return "number"
      // case "count": // Deprecated
      //   return "count";
      case "variable":
        var variable = _.findWhere(this.variables, { id: expr.variableId })
        if (!variable) {
          return null
        }
        return variable.type
      case "extension":
        return getExprExtension(expr.extension).getExprType(expr, this.schema, this.variables)
      default:
        // TODO remove
        if ((expr.type as any) == "count") {
          return "count" as any
        }

        throw new Error(`Not implemented for ${expr.type}`)
    }
  }

  /** Determines the aggregation status of an expression. This is whether the expression is
   * aggregate (like sum, avg, etc) or individual (a regular field-containing expression) or
   * literal (which is neither, just a number or text).
   * Invisible second parameter is depth to prevent infinite recursion */
  getExprAggrStatus(expr: Expr, _depth?: number): AggrStatus | null {
    if (expr == null || !expr.type) {
      return null
    }

    const depth = _depth || 0
    if (depth > 100) {
      throw new Error("Infinite recursion")
    }

    // Gets the aggregation status of a series of expressions (takes highest always)
    const getListAggrStatus = (exprs: Expr[]) => {
      // If has no expressions, is literal
      if (exprs.length === 0) {
        return "literal"
      }

      // Get highest type
      const aggrStatuses = _.map(exprs, (subExpr) => this.getExprAggrStatus(subExpr, depth + 1))
      if (aggrStatuses.includes("aggregate")) {
        return "aggregate"
      }
      if (aggrStatuses.includes("individual")) {
        return "individual"
      }
      if (aggrStatuses.includes("literal")) {
        return "literal"
      }
      return null
    }

    switch (expr.type) {
      case "id":
      case "scalar":
        return "individual"
      case "field":
        var column = this.schema.getColumn(expr.table, expr.column)
        if (column && column.expr) {
          // This is a slow operation for complex columns. Use weak cache
          // to cache column expression aggregate status
          return exprAggrStatusWeakCache.cacheFunction([this.schema, column.expr], [this.variables], () => {
            return this.getExprAggrStatus(column!.expr!, depth + 1)
          })
        }

        return "individual"
      case "op":
        // If aggregate op
        if (ExprUtils.isOpAggr(expr.op)) {
          return "aggregate"
        }

        return getListAggrStatus(expr.exprs)
      case "literal":
        return "literal"
      case "case":
        // Gather all exprs
        var exprs = [expr.else]
        exprs = exprs.concat(_.map(expr.cases, (cs) => cs.when))
        exprs = exprs.concat(_.map(expr.cases, (cs) => cs.then))
        return getListAggrStatus(exprs)
      case "score":
        return this.getExprAggrStatus(expr.input, depth + 1)
      case "build enumset":
        // Gather all exprs
        exprs = _.values(expr.values)
        return getListAggrStatus(exprs)
      case "count":
      case "comparison":
      case "logical": // Deprecated
        return "individual"
      case "variable":
        var variable = _.findWhere(this.variables, { id: expr.variableId })
        if (!variable) {
          return "literal" // To prevent crash in cleaning, return something
        }
        if (variable.table) {
          return "individual"
        }
        return "literal"
      case "extension":
        return getExprExtension(expr.extension).getExprAggrStatus(expr, this.schema, this.variables)
      // default:
      //   throw new Error(`Not implemented for ${expr.type}`);
    }
  }

  /** Determines if an set of joins are valid */
  areJoinsValid(table: string, joins: string[]): boolean {
    let t = table
    for (let j of joins) {
      const joinCol = this.schema.getColumn(t, j)
      if (!joinCol) {
        return false
      }

      if (["id", "id[]"].includes(joinCol.type)) {
        t = joinCol.idTable!
      } else if (joinCol.type === "join") {
        t = joinCol.join!.toTable
      } else {
        return false
      }
    }

    return true
  }

  // Gets the expression table
  getExprTable(expr: Expr) {
    if (!expr || expr.type == "literal") {
      return null
    }

    return expr.table
  }

  // Gets the types that can be formed by aggregating an expression
  getAggrTypes(expr: Expr) {
    const exprTable = this.getExprTable(expr)
    const aggrOpItems = this.findMatchingOpItems({
      lhsExpr: expr,
      aggr: true,
      ordered: exprTable ? this.schema.getTable(exprTable)?.ordering != null : false
    })
    return _.uniq(_.pluck(aggrOpItems, "resultType"))
  }

  localizeString(name: LocalizedString | string | null | undefined, locale?: string | null): string {
    return ExprUtils.localizeString(name, locale)
  }

  // Localize a string that is { en: "english word", etc. }. Works with null and plain strings too, returning always a string ("" for null)
  static localizeString(name: LocalizedString | string | null | undefined, locale?: string | null): string {
    if (!name) {
      return ""
    }

    // Simple string
    if (typeof name === "string") {
      return name
    }

    if (locale && name[locale] != null) {
      return name[locale]
    }

    if (name._base && name[name._base] != null) {
      return name[name._base]
    }

    // Fall back to English
    if (name.en != null) {
      return name.en
    }

    return ""
  }

  // Combine n expressions together by and
  static andExprs(table: string, ...exprs: Expr[]) {
    var exprsMapped = _.map(exprs, function (expr) {
      if (expr?.type === "op" && expr.op === "and") {
        return expr.exprs
      } else {
        return expr
      }
    })
    exprsMapped = _.compact(_.flatten(exprsMapped))
    if (exprsMapped.length === 0) {
      return null
    }
    if (exprsMapped.length === 1) {
      return exprsMapped[0]
    }

    return { type: "op", op: "and", table, exprs: exprsMapped }
  }

  /** Summarizes expression as text */
  summarizeExpr(expr: Expr, locale?: string): string {
    if (!expr) {
      return "None" // TODO localize
    }

    switch (expr.type) {
      case "scalar":
        return this.summarizeScalarExpr(expr, locale)
      case "field":
        return this.localizeString(this.schema.getColumn(expr.table, expr.column)?.name, locale) || ""
      case "id":
        return this.localizeString(this.schema.getTable(expr.table)?.name, locale) || ""
      case "op":
        // Special case for contains/intersects with literal RHS
        if (expr.op === "contains" && expr.exprs[1]?.type === "literal" && expr.exprs[1]?.valueType === "enumset") {
          return (
            this.summarizeExpr(expr.exprs[0], locale) +
            " includes all of " +
            this.stringifyLiteralValue("enumset", expr.exprs[1].value, locale, this.getExprEnumValues(expr.exprs[0]))
          )
        }

        if (expr.op === "intersects" && expr.exprs[1]?.type === "literal" && expr.exprs[1]?.valueType === "enumset") {
          return (
            this.summarizeExpr(expr.exprs[0], locale) +
            " includes any of " +
            this.stringifyLiteralValue("enumset", expr.exprs[1].value, locale, this.getExprEnumValues(expr.exprs[0]))
          )
        }

        // Special case for = any with literal RHS
        if (expr.op === "= any" && expr.exprs[1]?.type === "literal" && expr.exprs[1]?.valueType === "enumset") {
          return (
            this.summarizeExpr(expr.exprs[0], locale) +
            " is any of " +
            this.stringifyLiteralValue("enumset", expr.exprs[1].value, locale, this.getExprEnumValues(expr.exprs[0]))
          )
        }

        // Special case for = with literal RHS
        if (expr.op === "=" && expr.exprs[1]?.type === "literal" && expr.exprs[1]?.valueType === "enum") {
          return (
            this.summarizeExpr(expr.exprs[0], locale) +
            " is " +
            this.stringifyLiteralValue("enum", expr.exprs[1].value, locale, this.getExprEnumValues(expr.exprs[0]))
          )
        }

        // Special case for <> with literal RHS
        if (expr.op === "<>" && expr.exprs[1]?.type === "literal" && expr.exprs[1]?.valueType === "enum") {
          return (
            this.summarizeExpr(expr.exprs[0], locale) +
            " is not " +
            this.stringifyLiteralValue("enum", expr.exprs[1].value, locale, this.getExprEnumValues(expr.exprs[0]))
          )
        }

        // Special case for count
        if (expr.op === "count") {
          return "Number of " + this.localizeString(this.schema.getTable(expr.table!)?.name || "NOT FOUND", locale)
        }

        var opItem = this.findMatchingOpItems({ op: expr.op })[0]
        if (opItem) {
          if (opItem.prefix) {
            return (
              (opItem.prefixLabel || opItem.name) +
              " " +
              _.map(expr.exprs, (e, index) => {
                // Only use rhs placeholder if > 0
                if (index === 0) {
                  if (e) {
                    return this.summarizeExpr(e, locale)
                  } else {
                    return opItem.lhsPlaceholder || "None"
                  }
                } else {
                  if (e) {
                    return this.summarizeExpr(e, locale)
                  } else {
                    return opItem.rhsPlaceholder || "None"
                  }
                }
              }).join(opItem.joiner ? ` ${opItem.joiner} ` : ", ")
            )
          }

          if (expr.exprs.length === 1) {
            return this.summarizeExpr(expr.exprs[0], locale) + " " + opItem.name
          }

          return _.map(expr.exprs, (e) => this.summarizeExpr(e, locale)).join(" " + opItem.name + " ")
        } else {
          return ""
        }

      case "case":
        return this.summarizeCaseExpr(expr, locale)
      case "literal":
        return expr.value + ""
      case "score":
        return "Score of " + this.summarizeExpr(expr.input, locale)
      case "build enumset":
        return "Build Enumset"
      case "count":
        return "Count" // Deprecated
      case "variable":
        var variable = _.findWhere(this.variables, { id: expr.variableId })
        return variable ? this.localizeString(variable.name, locale) || "" : ""
      case "extension":
        return getExprExtension(expr.extension).summarizeExpr(expr, locale, this.schema, this.variables)
      default:
        throw new Error(`Unsupported type ${expr.type}`)
    }
  }

  summarizeScalarExpr(expr: ScalarExpr, locale?: string) {
    const exprType = this.getExprType(expr.expr)

    let str = ""

    // Add joins
    let t = expr.table
    for (let join of expr.joins) {
      const joinCol = this.schema.getColumn(t, join)
      if (joinCol) {
        str += this.localizeString(joinCol.name, locale) + " > "
      } else {
        str += "NOT FOUND > "
        break
      }
      if (joinCol.type === "join") {
        t = joinCol.join!.toTable
      } else if (["id", "id[]"].includes(joinCol.type)) {
        t = joinCol.idTable!
      } else {
        str += "INVALID >"
        break
      }
    }

    // Special case for id type to be rendered as {last join name}
    if (expr.expr?.type === "id" && !expr.aggr) {
      str = str.substring(0, str.length - 3)
    } else {
      let innerExpr = expr.expr

      // Handle legacy
      if (expr.aggr) {
        innerExpr = { type: "op", op: expr.aggr, table: (expr as any).expr?.table, exprs: [expr.expr] }
      }

      str += this.summarizeExpr(innerExpr, locale)
    }

    return str
  }

  summarizeCaseExpr(expr: CaseExpr, locale?: string) {
    let str = "If"
    for (let c of expr.cases) {
      str += " " + this.summarizeExpr(c.when)
      str += " Then " + this.summarizeExpr(c.then)
    }

    if (expr.else) {
      str += " Else " + this.summarizeExpr(expr.else)
    }

    return str
  }

  /** Converts a literal value related to an expression to a string, using name of enums. preferEnumCodes tries to use code over name */
  stringifyExprLiteral(expr: Expr, literal: any, locale?: string, preferEnumCodes?: boolean): string {
    return this.stringifyLiteralValue(
      this.getExprType(expr) as LiteralType,
      literal,
      locale,
      this.getExprEnumValues(expr),
      preferEnumCodes
    )
  }

  // Stringify a literal value of a certain type
  // type is "text", "number", etc.
  // Does not have intelligence to properly handle type id and id[], so just puts in raw id
  stringifyLiteralValue(
    type: LiteralType,
    value: any,
    locale?: string,
    enumValues?: EnumValue[] | null,
    preferEnumCodes?: boolean
  ) {
    if (value == null) {
      return "None" // TODO localize
    }

    switch (type) {
      case "text":
        return value
      case "number":
        return "" + value
      case "enum":
        // Get enumValues
        var item = _.findWhere(enumValues!, { id: value })
        if (item) {
          if (preferEnumCodes && item.code) {
            return item.code
          }
          return ExprUtils.localizeString(item.name, locale)
        }
        return "???"
      case "enumset":
        return _.map(value, (val) => {
          item = _.findWhere(enumValues!, { id: val })
          if (item) {
            if (preferEnumCodes && item.code) {
              return item.code
            }
            return ExprUtils.localizeString(item.name, locale)
          }
          return "???"
        }).join(", ")

      case "text[]":
        // Parse if string
        if (_.isString(value)) {
          value = JSON.parse(value || "[]")
        }

        return value.join(", ")

      case "date":
        return moment(value, moment.ISO_8601).format("ll")

      case "datetime":
        return moment(value, moment.ISO_8601).format("lll")
    }

    if (value === true) {
      return "True"
    }

    if (value === false) {
      return "False"
    }

    return `${value}`
  }

  /** Get all comparison ops (id and name) for a given left hand side type DEPRECATED
   * @deprecated
   */
  getComparisonOps(lhsType: any) {
    const ops = []
    switch (lhsType) {
      case "number":
        ops.push({ id: "=", name: "equals" })
        ops.push({ id: ">", name: "is greater than" })
        ops.push({ id: ">=", name: "is greater or equal to" })
        ops.push({ id: "<", name: "is less than" })
        ops.push({ id: "<=", name: "is less than or equal to" })
        break
      case "text":
        ops.push({ id: "= any", name: "is one of" })
        ops.push({ id: "=", name: "is" })
        ops.push({ id: "~*", name: "matches" })
        break
      case "date":
      case "datetime":
        ops.push({ id: "between", name: "between" })
        ops.push({ id: ">", name: "after" })
        ops.push({ id: "<", name: "before" })
        break
      case "enum":
        ops.push({ id: "= any", name: "is one of" })
        ops.push({ id: "=", name: "is" })
        break
      case "boolean":
        ops.push({ id: "= true", name: "is true" })
        ops.push({ id: "= false", name: "is false" })
        break
    }

    ops.push({ id: "is null", name: "has no value" })
    ops.push({ id: "is not null", name: "has a value" })

    return ops
  }

  /** Get the right hand side type for a comparison DEPRECATED
   * @deprecated
   */
  getComparisonRhsType(lhsType: any, op: any) {
    if (["= true", "= false", "is null", "is not null"].includes(op)) {
      return null
    }

    if (["= any"].includes(op)) {
      if (lhsType === "enum") {
        return "enum[]"
      } else if (lhsType === "text") {
        return "text[]"
      } else {
        throw new Error("Invalid lhs type for op = any")
      }
    }

    if (op === "between") {
      if (lhsType === "date") {
        return "daterange"
      }
      if (lhsType === "datetime") {
        return "datetimerange"
      } else {
        throw new Error("Invalid lhs type for op between")
      }
    }

    return lhsType
  }

  /** Get a list of fields that are referenced in a an expression
   * Useful to know which fields and joins are used. Includes joins as fields
   */
  getReferencedFields(expr: Expr): FieldExpr[] {
    let column, table
    let cols: FieldExpr[] = []

    if (!expr) {
      return cols
    }

    switch (expr.type) {
      case "field":
        cols.push(expr)
        column = this.schema.getColumn(expr.table, expr.column)
        if (column?.expr) {
          cols = cols.concat(this.getReferencedFields(column.expr))
        }
        break
      case "op":
        for (let subexpr of expr.exprs) {
          cols = cols.concat(this.getReferencedFields(subexpr))
        }
        break
      case "case":
        for (let subcase of expr.cases) {
          cols = cols.concat(this.getReferencedFields(subcase.when))
          cols = cols.concat(this.getReferencedFields(subcase.then))
        }
        cols = cols.concat(this.getReferencedFields(expr.else))
        break
      case "scalar":
        for (let join of expr.joins) {
          cols.push({ type: "field", table: expr.table, column: join })
          column = this.schema.getColumn(expr.table, join)
          // Handle gracefully
          if (!column) {
            break
          }

          if (column.type === "join") {
            table = column.join!.toTable
          } else if (["id", "id[]"].includes(column.type)) {
            table = column.idTable
          } else {
            break
          }
        }

        cols = cols.concat(this.getReferencedFields(expr.expr))
        break

      case "score":
        cols = cols.concat(this.getReferencedFields(expr.input))
        for (const value of _.values(expr.scores) as Expr[]) {
          cols = cols.concat(this.getReferencedFields(value))
        }
        break

      case "build enumset":
        for (const value of _.values(expr.values) as Expr[]) {
          cols = cols.concat(this.getReferencedFields(value))
        }
        break

      case "extension":
        cols = cols.concat(getExprExtension(expr.extension).getReferencedFields(expr, this.schema, this.variables))
        break
    }

    return _.uniq(cols, (col) => col.table + "/" + col.column)
  }

  /** Replace variables with literal values */
  inlineVariableValues(expr: Expr, variableValues: { [variableId: string]: Expr }): Expr {
    // Replace every part of an object, including array members
    const mapObject = function (obj: any, replacer: (input: any) => any) {
      if (!obj) {
        return obj
      }
      if (_.isArray(obj)) {
        return _.map(obj, replacer)
      }
      if (_.isObject(obj)) {
        return _.mapValues(obj, replacer)
      }
      return obj
    }

    var replacer = (part: any) => {
      part = mapObject(part, replacer)
      if (part && part.type === "variable") {
        // Find variable
        const variable = _.findWhere(this.variables, { id: part.variableId })
        if (!variable) {
          throw new Error(`Variable ${part.variableId} not found`)
        }
        return mapObject(variableValues[variable.id] || null, replacer)
      }
      return part
    }

    return mapObject(expr, replacer)
  }
}

// # Get a list of column ids of expression table that are referenced in a an expression
// # Useful to know which fields and joins are used. Does not follow joins, beyond including
// # the first join (which is a column in the start table).
// # Function does not require a schema, so schema can be null/undefined in constructor
// getImmediateReferencedColumns: (expr) ->
//   cols = []

//   if not expr
//     return cols

//   switch expr.type
//     when "field"
//       cols.push(expr.column)
//     when "op"
//       for subexpr in expr.exprs
//         cols = cols.concat(@getImmediateReferencedColumns(subexpr))
//     when "case"
//       for subcase in expr.cases
//         cols = cols.concat(@getImmediateReferencedColumns(subcase.when))
//         cols = cols.concat(@getImmediateReferencedColumns(subcase.then))
//       cols = cols.concat(@getImmediateReferencedColumns(expr.else))

//   return _.uniq(cols)

// Setup op items
// opItems are a list of ops for various types:
// op: e.g. "="
// name: e.g. "is"
// resultType: resulting type from op. e.g. "boolean"
// exprTypes: array of types of expressions required for arguments
// moreExprType: type of n more expressions (like "and" that takes n arguments)
// prefix: true if name goes before LHS value
// prefixLabel: overrides name when displayed as prefix
// lhsCond: optional condition function on LHS expr that tests if applicable (for "within" which only applies to hierarchical tables)
// rhsLiteral: prefer rhs literal
// joiner: string to put between exprs when prefix type
// aggr: true if aggregating (e.g. sum)
// ordered: for aggr = true if table must be have ordering
// lhsPlaceholder: placeholder for lhs expression
// rhsPlaceholder: placeholder for rhs expression
var opItems: OpItem[] = []

// Which op items are aggregate (key = op, value = true)
var aggrOpItems = {}

// opItems grouped by op
var groupedOpItems = {}

// Adds an op item (particular combination of operands types with an operator)
// exprTypes is a list of types for expressions. moreExprType is the type of further N expressions, if allowed
const addOpItem = (item: OpItem) => {
  opItems.push(_.defaults(item, { prefix: false, rhsLiteral: true, aggr: false, ordered: false }))
  if (item.aggr) {
    aggrOpItems[item.op] = true
  }

  const list = groupedOpItems[item.op] || []
  list.push(item)
  return (groupedOpItems[item.op] = list)
}

// TODO n?
addOpItem({ op: "= any", name: "is any of", resultType: "boolean", exprTypes: ["text", "text[]"] })
addOpItem({ op: "= any", name: "is any of", resultType: "boolean", exprTypes: ["enum", "enumset"] })

addOpItem({ op: "contains", name: "includes all of", resultType: "boolean", exprTypes: ["enumset", "enumset"] })
addOpItem({ op: "intersects", name: "includes any of", resultType: "boolean", exprTypes: ["enumset", "enumset"] })

addOpItem({ op: "contains", name: "includes all of", resultType: "boolean", exprTypes: ["text[]", "text[]"] })
addOpItem({ op: "intersects", name: "includes any of", resultType: "boolean", exprTypes: ["text[]", "text[]"] })

// Add relative dates
const relativeDateOps = [
  ["thisyear", "is this year"],
  ["lastyear", "is last year"],
  ["thismonth", "is this month"],
  ["lastmonth", "is last month"],
  ["today", "is today"],
  ["yesterday", "is yesterday"],
  ["last24hours", "is in last 24 hours"],
  ["last7days", "is in last 7 days"],
  ["last30days", "is in last 30 days"],
  ["last365days", "is in last 365 days"],
  ["last3months", "is in last 3 months"],
  ["last6months", "is in last 6 months"],
  ["last12months", "is in last 12 months"],
  ["future", "is in the future"],
  ["notfuture", "is not in the future"]
]
for (let relativeDateOp of relativeDateOps) {
  addOpItem({ op: relativeDateOp[0], name: relativeDateOp[1], resultType: "boolean", exprTypes: ["date"] })
  addOpItem({ op: relativeDateOp[0], name: relativeDateOp[1], resultType: "boolean", exprTypes: ["datetime"] })
}

// Add in ranges
addOpItem({ op: "between", name: "is between", resultType: "boolean", exprTypes: ["date", "date", "date"] })
addOpItem({ op: "between", name: "is between", resultType: "boolean", exprTypes: ["datetime", "datetime", "datetime"] })

addOpItem({ op: "=", name: "is", resultType: "boolean", exprTypes: ["number", "number"] })
addOpItem({ op: "=", name: "is", resultType: "boolean", exprTypes: ["text", "text"] })
addOpItem({ op: "=", name: "is", resultType: "boolean", exprTypes: ["enum", "enum"] })
addOpItem({ op: "=", name: "is", resultType: "boolean", exprTypes: ["date", "date"] })
addOpItem({ op: "=", name: "is", resultType: "boolean", exprTypes: ["datetime", "datetime"] })
addOpItem({ op: "=", name: "is", resultType: "boolean", exprTypes: ["boolean", "boolean"] })

addOpItem({ op: "<>", name: "is not", resultType: "boolean", exprTypes: ["text", "text"] })
addOpItem({ op: "<>", name: "is not", resultType: "boolean", exprTypes: ["enum", "enum"] })
addOpItem({ op: "<>", name: "is not", resultType: "boolean", exprTypes: ["date", "date"] })
addOpItem({ op: "<>", name: "is not", resultType: "boolean", exprTypes: ["datetime", "datetime"] })
addOpItem({ op: "<>", name: "is not", resultType: "boolean", exprTypes: ["boolean", "boolean"] })

addOpItem({ op: "<>", name: "is not", resultType: "boolean", exprTypes: ["number", "number"] })
addOpItem({ op: ">", name: "is greater than", resultType: "boolean", exprTypes: ["number", "number"] })
addOpItem({ op: "<", name: "is less than", resultType: "boolean", exprTypes: ["number", "number"] })
addOpItem({ op: ">=", name: "is greater than or equal to", resultType: "boolean", exprTypes: ["number", "number"] })
addOpItem({ op: "<=", name: "is less than or equal to", resultType: "boolean", exprTypes: ["number", "number"] })

for (const type1 of ["date", "datetime"] as LiteralType[]) {
  for (const type2 of ["date", "datetime"] as LiteralType[]) {
    addOpItem({ op: ">", name: "is after", resultType: "boolean", exprTypes: [type1, type2] })
    addOpItem({ op: "<", name: "is before", resultType: "boolean", exprTypes: [type1, type2] })
    addOpItem({ op: ">=", name: "is after or same as", resultType: "boolean", exprTypes: [type1, type2] })
    addOpItem({ op: "<=", name: "is before or same as", resultType: "boolean", exprTypes: [type1, type2] })
  }
}

addOpItem({ op: "between", name: "is between", resultType: "boolean", exprTypes: ["number", "number", "number"] })

addOpItem({
  op: "round",
  name: "Round",
  desc: "Round a number to closest whole number",
  resultType: "number",
  exprTypes: ["number"],
  prefix: true
})
addOpItem({
  op: "floor",
  name: "Floor",
  desc: "Round a number down",
  resultType: "number",
  exprTypes: ["number"],
  prefix: true
})
addOpItem({
  op: "ceiling",
  name: "Ceiling",
  desc: "Round a number up",
  resultType: "number",
  exprTypes: ["number"],
  prefix: true
})
addOpItem({
  op: "latitude",
  name: "Latitude of",
  desc: "Get latitude in degrees of a location",
  resultType: "number",
  exprTypes: ["geometry"],
  prefix: true
})
addOpItem({
  op: "longitude",
  name: "Longitude of",
  desc: "Get longitude in degrees of a location",
  resultType: "number",
  exprTypes: ["geometry"],
  prefix: true
})
addOpItem({
  op: "altitude",
  name: "Altitude of",
  desc: "Get altitude of a location in meters",
  resultType: "number",
  exprTypes: ["geometry"],
  prefix: true
})
addOpItem({
  op: "distance",
  name: "Distance between",
  desc: "Get distance in meters between two locations",
  resultType: "number",
  exprTypes: ["geometry", "geometry"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
})

// And/or is a list of booleans
addOpItem({ op: "and", name: "and", resultType: "boolean", exprTypes: [], moreExprType: "boolean" })
addOpItem({ op: "or", name: "or", resultType: "boolean", exprTypes: [], moreExprType: "boolean" })

for (const op of ["+", "*"]) {
  addOpItem({ op, name: op, resultType: "number", exprTypes: [], moreExprType: "number" })
}

addOpItem({ op: "-", name: "-", resultType: "number", exprTypes: ["number", "number"] })
addOpItem({ op: "/", name: "/", resultType: "number", exprTypes: ["number", "number"] })

// Date subtraction
addOpItem({
  op: "days difference",
  name: "Days between",
  desc: "Get the number of days between two dates",
  resultType: "number",
  exprTypes: ["date", "date"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
})
addOpItem({
  op: "days difference",
  name: "Days between",
  desc: "Get the number of days between two dates",
  resultType: "number",
  exprTypes: ["datetime", "datetime"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
})
addOpItem({
  op: "days difference",
  name: "Days between",
  desc: "Get the number of days between two dates",
  resultType: "number",
  exprTypes: ["date", "datetime"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
})
addOpItem({
  op: "days difference",
  name: "Days between",
  desc: "Get the number of days between two dates",
  resultType: "number",
  exprTypes: ["datetime", "date"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
})

addOpItem({
  op: "months difference",
  name: "Months between",
  desc: "Get the number of months between two dates",
  resultType: "number",
  exprTypes: ["date", "date"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
})
addOpItem({
  op: "months difference",
  name: "Months between",
  desc: "Get the number of months between two dates",
  resultType: "number",
  exprTypes: ["datetime", "datetime"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
})
addOpItem({
  op: "months difference",
  name: "Months between",
  desc: "Get the number of months between two dates",
  resultType: "number",
  exprTypes: ["date", "datetime"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
})
addOpItem({
  op: "months difference",
  name: "Months between",
  desc: "Get the number of months between two dates",
  resultType: "number",
  exprTypes: ["datetime", "date"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
})

addOpItem({
  op: "years difference",
  name: "Years between",
  desc: "Get the number of years between two dates",
  resultType: "number",
  exprTypes: ["date", "date"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
})
addOpItem({
  op: "years difference",
  name: "Years between",
  desc: "Get the number of years between two dates",
  resultType: "number",
  exprTypes: ["datetime", "datetime"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
})
addOpItem({
  op: "years difference",
  name: "Years between",
  desc: "Get the number of years between two dates",
  resultType: "number",
  exprTypes: ["date", "datetime"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
})
addOpItem({
  op: "years difference",
  name: "Years between",
  desc: "Get the number of years between two dates",
  resultType: "number",
  exprTypes: ["datetime", "date"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
})

addOpItem({
  op: "days since",
  name: "Days since",
  desc: "Get number of days from a date to the present",
  resultType: "number",
  exprTypes: ["date"],
  prefix: true,
  rhsLiteral: false
})
addOpItem({
  op: "days since",
  name: "Days since",
  desc: "Get number of days from a date to the present",
  resultType: "number",
  exprTypes: ["datetime"],
  prefix: true,
  rhsLiteral: false
})

addOpItem({
  op: "month",
  name: "Month",
  desc: "Month of year",
  resultType: "enum",
  exprTypes: ["date"],
  prefix: true,
  rhsLiteral: false
})
addOpItem({
  op: "month",
  name: "Month",
  desc: "Month of year",
  resultType: "enum",
  exprTypes: ["datetime"],
  prefix: true,
  rhsLiteral: false
})

addOpItem({
  op: "yearmonth",
  name: "Year and Month",
  desc: "Date of start of month",
  resultType: "date",
  exprTypes: ["date"],
  prefix: true,
  rhsLiteral: false
})
addOpItem({
  op: "yearmonth",
  name: "Year and Month",
  desc: "Date of start of month",
  resultType: "date",
  exprTypes: ["datetime"],
  prefix: true,
  rhsLiteral: false
})

addOpItem({
  op: "year",
  name: "Year",
  desc: "Date of start of year",
  resultType: "date",
  exprTypes: ["date"],
  prefix: true,
  rhsLiteral: false
})
addOpItem({
  op: "year",
  name: "Year",
  desc: "Date of start of year",
  resultType: "date",
  exprTypes: ["datetime"],
  prefix: true,
  rhsLiteral: false
})

addOpItem({
  op: "weekofmonth",
  name: "Week of month",
  desc: "Week within the month",
  resultType: "enum",
  exprTypes: ["date"],
  prefix: true,
  rhsLiteral: false
})
addOpItem({
  op: "weekofmonth",
  name: "Week of month",
  desc: "Week within the month",
  resultType: "enum",
  exprTypes: ["datetime"],
  prefix: true,
  rhsLiteral: false
})

addOpItem({
  op: "dayofmonth",
  name: "Day of month",
  desc: "Day within the month (1-31)",
  resultType: "enum",
  exprTypes: ["date"],
  prefix: true,
  rhsLiteral: false
})
addOpItem({
  op: "dayofmonth",
  name: "Day of month",
  desc: "Day within the month (1-31)",
  resultType: "enum",
  exprTypes: ["datetime"],
  prefix: true,
  rhsLiteral: false
})

addOpItem({
  op: "yearquarter",
  name: "Year/Quarter",
  desc: "Year and quarter of a date",
  resultType: "enum",
  exprTypes: ["date"],
  prefix: true,
  rhsLiteral: false
})
addOpItem({
  op: "yearquarter",
  name: "Year/Quarter",
  desc: "Year and quarter of a date",
  resultType: "enum",
  exprTypes: ["datetime"],
  prefix: true,
  rhsLiteral: false
})

addOpItem({
  op: "yearweek",
  name: "Year/Week",
  desc: "Year and week of a date",
  resultType: "enum",
  exprTypes: ["date"],
  prefix: true,
  rhsLiteral: false
})
addOpItem({
  op: "yearweek",
  name: "Year/Week",
  desc: "Year and week of a date",
  resultType: "enum",
  exprTypes: ["datetime"],
  prefix: true,
  rhsLiteral: false
})

addOpItem({
  op: "weekofyear",
  name: "Week",
  desc: "Week of a date",
  resultType: "enum",
  exprTypes: ["date"],
  prefix: true,
  rhsLiteral: false
})
addOpItem({
  op: "weekofyear",
  name: "Week",
  desc: "Week of a date",
  resultType: "enum",
  exprTypes: ["datetime"],
  prefix: true,
  rhsLiteral: false
})

addOpItem({
  op: "within",
  name: "is within",
  resultType: "boolean",
  exprTypes: ["id", "id"],
  lhsCond: (lhsExpr, exprUtils) => {
    const lhsIdTable = exprUtils.getExprIdTable(lhsExpr)
    if (lhsIdTable) {
      return (
        exprUtils.schema.getTable(lhsIdTable)!.ancestry != null ||
        exprUtils.schema.getTable(lhsIdTable)!.ancestryTable != null
      )
    }
    return false
  }
})

addOpItem({ op: "=", name: "is", resultType: "boolean", exprTypes: ["id", "id"] })
addOpItem({ op: "<>", name: "is not", resultType: "boolean", exprTypes: ["id", "id"] })
addOpItem({ op: "= any", name: "is any of", resultType: "boolean", exprTypes: ["id", "id[]"] })

for (const type of [
  "text",
  "number",
  "enum",
  "enumset",
  "boolean",
  "date",
  "datetime",
  "geometry",
  "id",
  "image",
  "imagelist",
  "id[]",
  "text[]"
] as LiteralType[]) {
  addOpItem({
    op: "last",
    name: "Latest",
    desc: "Get latest value when there are multiple",
    resultType: type,
    exprTypes: [type],
    prefix: true,
    aggr: true,
    ordered: true
  })
  addOpItem({
    op: "last where",
    name: "Latest where",
    desc: "Get latest value that matches a condition",
    resultType: type,
    exprTypes: [type, "boolean"],
    prefix: true,
    prefixLabel: "Latest",
    aggr: true,
    ordered: true,
    rhsLiteral: false,
    joiner: "where",
    rhsPlaceholder: "All"
  })
  addOpItem({
    op: "previous",
    name: "Previous",
    desc: "Get 2nd latest value when there are multiple",
    resultType: type,
    exprTypes: [type],
    prefix: true,
    aggr: true,
    ordered: true
  })
  addOpItem({
    op: "first",
    name: "First",
    desc: "Get first value when there are multiple",
    resultType: type,
    exprTypes: [type],
    prefix: true,
    aggr: true,
    ordered: true
  })
  addOpItem({
    op: "first where",
    name: "First where",
    desc: "Get first value that matches a condition",
    resultType: type,
    exprTypes: [type, "boolean"],
    prefix: true,
    prefixLabel: "First",
    aggr: true,
    ordered: true,
    rhsLiteral: false,
    joiner: "where",
    rhsPlaceholder: "All"
  })
}

addOpItem({
  op: "sum",
  name: "Total",
  desc: "Add all values together",
  resultType: "number",
  exprTypes: ["number"],
  prefix: true,
  aggr: true
})
addOpItem({
  op: "avg",
  name: "Average",
  desc: "Average all values together",
  resultType: "number",
  exprTypes: ["number"],
  prefix: true,
  aggr: true
})

for (const type of ["number", "date", "datetime"] as LiteralType[]) {
  addOpItem({
    op: "min",
    name: "Minimum",
    desc: "Get smallest value",
    resultType: type,
    exprTypes: [type],
    prefix: true,
    aggr: true
  })
  addOpItem({
    op: "min where",
    name: "Minimum where",
    desc: "Get smallest value that matches a condition",
    resultType: type,
    exprTypes: [type, "boolean"],
    prefix: true,
    prefixLabel: "Minimum",
    aggr: true,
    rhsLiteral: false,
    joiner: "of",
    rhsPlaceholder: "All"
  })

  addOpItem({
    op: "max",
    name: "Maximum",
    desc: "Get largest value",
    resultType: type,
    exprTypes: [type],
    prefix: true,
    aggr: true
  })
  addOpItem({
    op: "max where",
    name: "Maximum where",
    desc: "Get largest value that matches a condition",
    resultType: type,
    exprTypes: [type, "boolean"],
    prefix: true,
    prefixLabel: "Maximum",
    aggr: true,
    rhsLiteral: false,
    joiner: "of",
    rhsPlaceholder: "All"
  })
}

addOpItem({
  op: "percent where",
  name: "Percent where",
  desc: "Get percent of items that match a condition",
  resultType: "number",
  exprTypes: ["boolean", "boolean"],
  prefix: true,
  aggr: true,
  rhsLiteral: false,
  joiner: "of",
  rhsPlaceholder: "All"
})
addOpItem({
  op: "count where",
  name: "Number where",
  desc: "Get number of items that match a condition",
  resultType: "number",
  exprTypes: ["boolean"],
  prefix: true,
  aggr: true
})
addOpItem({
  op: "sum where",
  name: "Total where",
  desc: "Add together only values that match a condition",
  resultType: "number",
  exprTypes: ["number", "boolean"],
  prefix: true,
  prefixLabel: "Total",
  aggr: true,
  rhsLiteral: false,
  joiner: "where",
  rhsPlaceholder: "All"
})

addOpItem({
  op: "within any",
  name: "is within any of",
  resultType: "boolean",
  exprTypes: ["id", "id[]"],
  lhsCond: (lhsExpr, exprUtils) => {
    const lhsIdTable = exprUtils.getExprIdTable(lhsExpr)
    if (lhsIdTable) {
      return (
        exprUtils.schema.getTable(lhsIdTable)?.ancestry != null ||
        exprUtils.schema.getTable(lhsIdTable)?.ancestryTable != null
      )
    }
    return false
  }
})

addOpItem({
  op: "array_agg",
  name: "Make list of",
  desc: "Aggregates results into a list",
  resultType: "text[]",
  exprTypes: ["text"],
  prefix: true,
  aggr: true
})

addOpItem({
  op: "concat",
  name: "Concatenate",
  desc: "Combine several text strings together",
  resultType: "text",
  exprTypes: ["text"],
  moreExprType: "text",
  prefix: true,
  joiner: " with "
})

addOpItem({ op: "contains", name: "includes all of", resultType: "boolean", exprTypes: ["id[]", "id[]"] })
addOpItem({ op: "intersects", name: "includes any of", resultType: "boolean", exprTypes: ["id[]", "id[]"] })
addOpItem({ op: "includes", name: "includes", resultType: "boolean", exprTypes: ["id[]", "id"] })

addOpItem({
  op: "count",
  name: "Total Number",
  desc: "Get total number of items",
  resultType: "number",
  exprTypes: [],
  prefix: true,
  aggr: true
})
addOpItem({
  op: "percent",
  name: "Percent of Total",
  desc: "Percent of all items",
  resultType: "number",
  exprTypes: [],
  prefix: true,
  aggr: true
})

addOpItem({ op: "~*", name: "matches", resultType: "boolean", exprTypes: ["text", "text"] })
addOpItem({
  op: "not",
  name: "Not",
  desc: "Opposite of a value",
  resultType: "boolean",
  exprTypes: ["boolean"],
  prefix: true
})
for (const type of [
  "text",
  "number",
  "enum",
  "enumset",
  "boolean",
  "date",
  "datetime",
  "geometry",
  "image",
  "imagelist",
  "id",
  "json",
  "dataurl"
] as LiteralType[]) {
  addOpItem({ op: "is null", name: "is blank", resultType: "boolean", exprTypes: [type] })
  addOpItem({ op: "is not null", name: "is not blank", resultType: "boolean", exprTypes: [type] })
}

for (const type of ["id", "text", "date"] as LiteralType[]) {
  addOpItem({
    op: "count distinct",
    name: "Number of unique",
    desc: "Count number of unique values",
    resultType: "number",
    exprTypes: [type],
    prefix: true,
    aggr: true
  })
}

addOpItem({
  op: "length",
  name: "Number of values in",
  desc: "Advanced: number of values selected in a multi-choice field",
  resultType: "number",
  exprTypes: ["enumset"],
  prefix: true
})
addOpItem({
  op: "length",
  name: "Number of values in",
  desc: "Advanced: number of images present",
  resultType: "number",
  exprTypes: ["imagelist"],
  prefix: true
})
addOpItem({
  op: "length",
  name: "Number of values in",
  desc: "Advanced: number of items present in a text list",
  resultType: "number",
  exprTypes: ["text[]"],
  prefix: true
})

addOpItem({
  op: "line length",
  name: "Length of line",
  desc: "Length of a line shape in meters",
  resultType: "number",
  exprTypes: ["geometry"],
  prefix: true
})

addOpItem({
  op: "is latest",
  name: "Is latest for each",
  desc: "Only include latest item for each of something",
  resultType: "boolean",
  exprTypes: ["id", "boolean"],
  prefix: true,
  ordered: true,
  aggr: false,
  rhsLiteral: false,
  joiner: "where",
  rhsPlaceholder: "All"
})

addOpItem({
  op: "current date",
  name: "Today",
  desc: "Advanced: current date. Do not use in comparisons",
  resultType: "date",
  exprTypes: [],
  prefix: true
})
addOpItem({
  op: "current datetime",
  name: "Now",
  desc: "Advanced: current datetime. Do not use in comparisons",
  resultType: "datetime",
  exprTypes: [],
  prefix: true
})

addOpItem({
  op: "to text",
  name: "Convert to text",
  desc: "Advanced: convert a choice, text list, or number type to a text value",
  resultType: "text",
  exprTypes: ["enum"],
  prefix: true
})
addOpItem({
  op: "to text",
  name: "Convert to text",
  desc: "Advanced: convert a choice, text list, or number type to a text value",
  resultType: "text",
  exprTypes: ["number"],
  prefix: true
})
addOpItem({
  op: "to text",
  name: "Convert to text",
  desc: "Advanced: convert a choice, text list, or number type to a text value",
  resultType: "text",
  exprTypes: ["text[]"],
  prefix: true
})

addOpItem({
  op: "to date",
  name: "Convert to date",
  desc: "Convert a datetime to a date",
  resultType: "date",
  exprTypes: ["datetime"],
  prefix: true
})

addOpItem({
  op: "to number",
  name: "Convert to number",
  desc: "Convert a text value to a number or null if not valid number",
  resultType: "number",
  exprTypes: ["text"],
  prefix: true
})

addOpItem({
  op: "least",
  name: "Least of",
  desc: "Takes the smallest of several numbers",
  resultType: "number",
  exprTypes: ["number", "number"],
  moreExprType: "number",
  prefix: true,
  joiner: ", "
})
addOpItem({
  op: "greatest",
  name: "Greatest of",
  desc: "Takes the largest of several numbers",
  resultType: "number",
  exprTypes: ["number", "number"],
  moreExprType: "number",
  prefix: true,
  joiner: ", "
})
