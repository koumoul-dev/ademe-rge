const assert = require('assert').strict
const { validateLiensLine } = require('../lib/import-validate')

// a fully valid liens.csv line, used as a baseline to mutate field by field
// columns: entreprise_id_organisme, qualification_code, date début, date fin,
//          url qualification, libellé certificat, particulier
const baseLine = () => ['ent1', 'QUALIF1', '20250101', '20251231', 'https://example.org/q', 'certificat', '1']
const baseContext = () => ({ idEntreprises: new Set(['ent1']), codeQualifs: new Set(['QUALIF1']) })

describe('validateLiensLine date validation', () => {
  it('accepts valid 8-digit calendar dates', () => {
    const errors = []
    validateLiensLine(baseLine(), 0, errors, baseContext())
    assert.deepEqual(errors, [])
  })

  it('rejects an impossible "date fin" that is 8 digits but not a real calendar date', () => {
    const line = baseLine()
    line[3] = '20250631' // June 31st does not exist
    const errors = []
    validateLiensLine(line, 0, errors, baseContext())
    assert.equal(errors.length, 1)
    assert.match(errors[0], /date fin/)
  })

  it('rejects a zero-filled "date fin" placeholder', () => {
    const line = baseLine()
    line[3] = '00000000'
    const errors = []
    validateLiensLine(line, 0, errors, baseContext())
    assert.equal(errors.length, 1)
    assert.match(errors[0], /date fin/)
  })

  it('rejects an impossible "date début"', () => {
    const line = baseLine()
    line[2] = '20250230' // February 30th does not exist
    const errors = []
    validateLiensLine(line, 0, errors, baseContext())
    assert.equal(errors.length, 1)
    assert.match(errors[0], /date début/)
  })
})
