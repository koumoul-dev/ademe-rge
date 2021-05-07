const parse = require('csv-parse/lib/sync')

const path = require('path')
const fs = require('fs-extra')
const qualifDomaineLines = parse(fs.readFileSync(path.join(__dirname, '../resources/RGE - Lien domaine qualification.csv')))
const qualifDomaine = qualifDomaineLines.reduce((a, qd) => { a[qd[3]] = qd; return a }, {})

// read the 3 files 'entreprises', 'qualifications' and 'liens' and build an object with a daily state
module.exports = async (axios, dataset, log) => {
  await log.info('Récupération de la liste des domaines inconnus')
  const missing = (await axios.get(`api/v1/datasets/${dataset.id}/values_agg`, { params: { field: 'code_qualification', domaine_in: 'Inconnu' } })).data.aggs
  if (missing.length) {
    await log.error(`${missing.length} domaines inconnus pour les qualifications : ${missing.map(m => m.value + ' (' + m.total + ')').join(', ')}`)
    for (const qualification of missing) {
      const domaine = qualifDomaine[qualification.value]
      if (domaine) {
        const toUpdate = (await axios.get(`api/v1/datasets/${dataset.id}/lines`, { params: { code_qualification_in: qualification.value, domaine_in: 'Inconnu', size: 10000, select: '_id' } })).data.results
        await log.error(`Mise à jour de ${toUpdate.length} lignes pour le domaine du code qualification : ${qualification.value}`)

        const bulk = toUpdate.map(d => ({
          _action: 'patch',
          _id: d._id,
          organisme: domaine[2] || 'Non renseigné',
          domaine: domaine[1] || 'Non renseigné',
          meta_domaine: domaine[4] || 'Non renseigné'
        }))
        await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines`, bulk)
      }
    }
  } else {
    await log.info('Aucun domaine inconnu')
  }
}
