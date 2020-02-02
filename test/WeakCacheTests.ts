import { assert } from 'chai'
import { WeakCache } from '../src/WeakCache'


const a = { x: 1 }
const b = { x: 2 }
const c = { x: 3 }
const d = { x: 4 }

describe("WeakCache", () => {
  it("gets single object", () => {
    const wc = new WeakCache()

    wc.set([a], [], "foo")
    assert.equal(wc.get([a], []), "foo")
    assert.equal(wc.get([b], []), undefined)    
  })

  it("gets multiple object", () => {
    const wc = new WeakCache()

    wc.set([a, b], [], "foo")
    assert.equal(wc.get([a, b], []), "foo")
    assert.equal(wc.get([b, a], []), undefined)
  })

  it("gets multiple object witn strong", () => {
    const wc = new WeakCache()

    wc.set([a, b], [{}, 123], "foo")
    assert.equal(wc.get([a, b], [{}, 123]), "foo")
    assert.equal(wc.get([a, b], [123, {}]), undefined)
    assert.equal(wc.get([b, a], [123, {}]), undefined)
  })

  it("caches a function", () => {
    const wc = new WeakCache()

    let count = 0
    const func = (obj: any) => {
      count += 1
      return obj.x
    }

    assert.equal(wc.cacheFunction([a], [], () => func(a)), 1)
    assert.equal(wc.cacheFunction([b], [], () => func(b)), 2)
    assert.equal(wc.cacheFunction([b], [], () => func(b)), 2)
    assert.equal(wc.cacheFunction([b], [], () => func(b)), 2)

    assert.equal(count, 2)
  })
})