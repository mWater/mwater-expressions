/** Cache which can cache by an array of weak objects and an array of strong primitives and
 * objects.
 */
export declare class WeakCache {
    cache: WeakMap<any, any>;
    constructor();
    /** Gets a cache entry. Pass array of objects to hold weakly and optional strong array.
     * Returns undefined if not found. Must always be called with same length in each array.
     */
    get(weak: any[], strong: any[]): any;
    /** Sets a cache entry. Pass array of objects to hold weakly and optional strong array.
     * Returns undefined if not found. Must always be called with same length in each array.
     */
    set(weak: any[], strong: any[], value: any): void;
    /** Looks up cache, and if not found, runs function and stores result */
    cacheFunction(weak: any[], strong: any[], func: () => any): any;
}
