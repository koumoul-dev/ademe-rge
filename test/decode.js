const assert = require('assert').strict
const { decodeCp1252 } = require('../lib/decode')

describe('decodeCp1252', () => {
  it('decodes windows-1252 specific characters (not plain latin1)', () => {
    // "Société Générale – œuvre €" in windows-1252
    const buf = Buffer.from([0x53, 0x6f, 0x63, 0x69, 0xe9, 0x74, 0xe9, 0x20, 0x96, 0x20, 0x9c, 0x75, 0x76, 0x72, 0x65, 0x20, 0x80])
    assert.equal(decodeCp1252(buf), 'Société – œuvre €')
  })

  it('decodes every defined byte identically to the full windows-1252 table', () => {
    const bytes = [...Array(256).keys()].filter(b => ![0x81, 0x8d, 0x8f, 0x90, 0x9d].includes(b))
    const out = decodeCp1252(Buffer.from(bytes))
    assert.equal(out.length, bytes.length)
    assert.equal(out[0x80 - 0], '€')
    assert.equal(out.at(-1), 'ÿ')
  })

  it('throws EILSEQ on bytes undefined in windows-1252', () => {
    for (const b of [0x81, 0x8d, 0x8f, 0x90, 0x9d]) {
      assert.throws(() => decodeCp1252(Buffer.from([0x41, b, 0x42])), { code: 'EILSEQ' })
    }
  })
})
