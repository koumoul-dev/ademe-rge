const assert = require('assert').strict
const { diffLines, updateOrgaDataset } = require('../lib/orga-dataset')

const row = (siret, code, extra = {}) => ({ siret, code_qualification: code, nom_entreprise: 'not sent', particulier: true, lien_date_debut: '2024-01-01', ...extra })
const line = (siret, code, extra = {}) => ({ _id: `${siret}-${code}`, siret, code_qualification: code, particulier: true, lien_date_debut: '2024-01-01', ...extra })

const log = { info: async () => {}, warning: async () => {}, error: async () => {} }
const fakeAxios = (dataset, pages = []) => {
  const calls = []
  return {
    calls,
    get: async (url, opts) => {
      calls.push(['get', url, opts?.params])
      if (url === `api/v1/datasets/${dataset.id}`) return { data: dataset }
      return { data: pages.shift() }
    },
    post: async (url, body) => {
      calls.push(['post', url, body])
      return { data: { nbOk: 1, nbErrors: 0, errors: [] } }
    }
  }
}

describe('diffLines', () => {
  it('sends only new and changed rows, and deletes the lines gone from the rows', () => {
    const lines = [
      line('1', 'A'),
      line('2', 'B', { email: 'old@x.fr' }),
      line('3', 'C'),
      line('4', 'D', { telephone: '' })
    ]
    const rows = [
      row('1', 'A'),
      row('2', 'B', { email: 'new@x.fr' }),
      row('4', 'D', { telephone: undefined }),
      row('5', 'E')
    ]
    const { bulk, stats } = diffLines(lines, rows)
    assert.deepEqual(stats, { upserted: 2, deleted: 1, unmodified: 2, duplicates: 0 })
    assert.deepEqual(bulk.map(op => [op._action, op._id ?? op.siret]), [['createOrUpdate', '2'], ['createOrUpdate', '5'], ['delete', '3-C']])
    assert.equal(bulk[0].email, 'new@x.fr')
    assert.ok(!('nom_entreprise' in bulk[0]), 'only the columns typed by the organisms are sent')
  })

  it('keeps the last of duplicated rows, so that the line is stable from one run to the next', () => {
    const rows = [row('1', 'A', { email: 'first@x.fr' }), row('1', 'A', { email: 'last@x.fr' })]
    const { stats } = diffLines([line('1', 'A', { email: 'last@x.fr' })], rows)
    assert.deepEqual(stats, { upserted: 0, deleted: 0, unmodified: 1, duplicates: 1 })
  })
})

describe('updateOrgaDataset', () => {
  it('reads every page of an editable dataset with the primary key and sends the differences only', async () => {
    const axios = fakeAxios({ id: 'd', isRest: true, primaryKey: ['siret', 'code_qualification'] }, [
      { results: [line('1', 'A')], next: 'http://df/next' },
      { results: [line('2', 'B')] }
    ])
    await updateOrgaDataset(axios, { id: 'd' }, [row('1', 'A'), row('3', 'C')], 'cnoa', log)
    assert.deepEqual(axios.calls.map(c => c[1]), ['api/v1/datasets/d', 'api/v1/datasets/d/lines', 'http://df/next', 'api/v1/datasets/d/_bulk_lines'])
    assert.deepEqual(axios.calls[3][2].map(op => op._action), ['createOrUpdate', 'delete'])
  })

  it('replaces every line of an editable dataset without the primary key', async () => {
    const axios = fakeAxios({ id: 'd', isRest: true })
    await updateOrgaDataset(axios, { id: 'd' }, [row('1', 'A')], 'cnoa', log)
    assert.equal(axios.calls[1][1], 'api/v1/datasets/d/_bulk_lines?drop=true')
    assert.equal(axios.calls[1][2][0]._action, 'create')
  })

  it('refuses a dataset that is neither editable nor a file dataset', async () => {
    const axios = fakeAxios({ id: 'd', title: 'Virtuel', isVirtual: true })
    await assert.rejects(updateOrgaDataset(axios, { id: 'd' }, [row('1', 'A')], 'cnoa', log), /ni éditable ni un jeu fichier/)
    assert.equal(axios.calls.length, 1)
  })

  it('uploads the whole file of a file dataset', async () => {
    const axios = fakeAxios({ id: 'd', file: { name: 'old.csv' } })
    await updateOrgaDataset(axios, { id: 'd' }, [row('1', 'A')], 'cnoa', log)
    const [, url, form] = axios.calls[1]
    assert.equal(url, 'api/v1/datasets/d')
    const file = form.get('file')
    assert.equal(file.name, 'cnoa.csv')
    assert.equal((await file.text()).split('\n')[0], 'siret,telephone,email,site_internet,code_qualification,lien_date_debut,lien_date_fin,url_qualification,nom_certificat,particulier')
  })
})
