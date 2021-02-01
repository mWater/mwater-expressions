import { Column, LocalizedString, Section } from './types';
export * from './types';
export { default as DataSource } from './DataSource';
export { default as ExprValidator, ValidateOptions } from './ExprValidator';
export { default as Schema } from './Schema';
export * from './PromiseExprEvaluator';
export { default as ExprUtils } from './ExprUtils';
export { WeakCache } from './WeakCache';
/** Cleans expressions. Cleaning means nulling invalid (not just incomplete) expressions if they cannot be auto-fixed. */
export { default as ExprCleaner, CleanExprOptions } from './ExprCleaner';
export { default as ExprCompiler } from './ExprCompiler';
export { default as PriorityDataQueue } from './PriorityDataQueue';
export { default as NullDataSource } from './NullDataSource';
export { default as ColumnNotFoundException } from './ColumnNotFoundException';
export * from './injectTableAliases';
export * from './extensions';
/** Flatten a list of contents to columns */
export declare function flattenContents(contents: (Column | Section)[]): Column[];
/** Localize a string that is { en: "english word", etc. }. Works with null and plain strings too. */
export declare function localizeString(name: LocalizedString | null | undefined | string, locale?: string): string | null | undefined;
