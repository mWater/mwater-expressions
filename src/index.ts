import { Column, Section } from './types'

export * from './types'
export { default as DataSource } from './DataSource'
export { default as ExprValidator } from './ExprValidator'
export { default as Schema } from './Schema'
export * from './PromiseExprEvaluator'

export { default as ExprUtils } from './ExprUtils'

export { WeakCache } from './WeakCache'

/** Cleans expressions. Cleaning means nulling invalid (not just incomplete) expressions if they cannot be auto-fixed. */
export { default as ExprCleaner } from './ExprCleaner'

export { default as ExprCompiler } from './ExprCompiler'

export { default as PriorityDataQueue } from './PriorityDataQueue'

export { default as NullDataSource } from './NullDataSource'

export { default as ColumnNotFoundException } from './ColumnNotFoundException'

export * from './injectTableAliases'

/** Flatten a list of contents to columns */
export function flattenContents(contents: (Column | Section)[]): Column[] {
  let columns: Column[] = []

  for (const item of contents) {
    if (item.type == "section") {
      columns = columns.concat(flattenContents(item.contents))
    }
    else {
      columns.push(item)
    }
  }

  return columns
}