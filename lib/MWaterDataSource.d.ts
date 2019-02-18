import { DataSource } from '../index'

/** Caching data source for mWater. Requires jQuery. require explicitly: require('mwater-expressions/lib/MWaterDataSource') */
export default class MWaterDataSource extends DataSource {
  /**
   * @param apiUrl 
   * @param options serverCaching: allows server to send cached results. default true
   * localCaching allows local MRU cache. default true
   * imageApiUrl: overrides apiUrl for images
   */
  constructor(apiUrl: string, options: { serverCaching?: boolean, localCaching?: boolean, imageApiUrl?: string })
}