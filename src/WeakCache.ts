import _ from 'lodash'

/** Cache which can cache by an array of weak objects and an array of strong primitives and 
 * objects.
 */
export class WeakCache {
  cache: WeakMap<any, any> 

  constructor() {
    this.cache = new WeakMap()
  }

  /** Gets a cache entry. Pass array of objects to hold weakly and optional strong array.
   * Returns undefined if not found. Must always be called with same length in each array.
   */
  get(weak: any[], strong: any[]): any {
    // Follow weak
    let value = this.cache
    
    for (const k of weak) {
      value = value.get(k)
      if (value === undefined) {
        return undefined
      }
    }

    value = value.get(JSON.stringify(strong))
    return value
  }

  /** Sets a cache entry. Pass array of objects to hold weakly and optional strong array.
   * Returns undefined if not found. Must always be called with same length in each array.
   */
  set(weak: any[], strong: any[], value: any) {
    // Follow weak
    let map = this.cache
    
    for (const k of _.initial(weak)) {
      if (!map.has(k)) {
        map.set(k, new WeakMap())
      }
      map = map.get(k)
    }

    if (!map.has(_.last(weak))) {
      map.set(_.last(weak), new Map())
    }
    map = map.get(_.last(weak))
    
    map.set(JSON.stringify(strong), value)
  }

  /** Looks up cache, and if not found, runs function and stores result */
  cacheFunction(weak: any[], strong: any[], func: () => any) {
    let value = this.get(weak, strong)
    if (value !== undefined) {
      return value
    }

    value = func()
    this.set(weak, strong, value)
    return value
  }
}