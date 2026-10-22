const assert = require('assert').strict
const { toCSV } = require('../lib/format')

describe('toCSV', () => {
  it('writes a header, empty cells for missing values and escapes special characters', () => {
    const rows = [
      { siret: '01234567890123', email: undefined, nom: 'A, "B"\nC', particulier: true },
      { siret: '98765432109876', email: 'a@b.fr', nom: 'D', particulier: false }
    ]
    assert.equal(toCSV(['siret', 'email', 'nom', 'particulier'], rows),
      'siret,email,nom,particulier\n' +
      '01234567890123,,"A, ""B""\nC",true\n' +
      '98765432109876,a@b.fr,D,false\n')
  })
})
