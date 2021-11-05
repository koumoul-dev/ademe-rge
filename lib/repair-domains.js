module.exports = async (axios, dataset, qualifDomaine, log) => {
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
          organisme: domaine.ORGANISME || 'Non renseigné',
          domaine: domaine.DOMAINE_TRAVAUX || 'Non renseigné',
          meta_domaine: domaine.TITRE_DOMAINE || 'Non renseigné'
        }))
        await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines`, bulk)
      }
    }
  } else {
    await log.info('Aucun domaine inconnu')
  }
}
