const checkFields = [
  'entreprise_id_organisme',
  'nom_entreprise',
  'adresse',
  'code_postal',
  'commune',
  'latitude',
  'longitude',
  'telephone',
  'email',
  'site_internet',
  'url_qualification',
  'nom_certificat',
  'particulier'
]

const formatDay = (day) => day.slice(0, 4) + '-' + day.slice(4, 6) + '-' + day.slice(6, 8)

module.exports = async (previousState, state, previousDay, day) => {
  const bulk = []
  const stats = { closed: 0, created: 0, unmodified: 0, updated: 0 }
  for (const key in previousState) {
    if (!state[key]) {
      bulk.push({ _action: 'patch', _id: key, traitement_date_fin: formatDay(previousDay) })
      stats.closed += 1
    }
  }
  for (const key in state) {
    if (!previousState[key]) {
      bulk.push({ _action: 'create', _id: key, traitement_date_debut: formatDay(day), ...state[key] })
      stats.created += 1
      continue
    }
    const changes = checkFields.filter(f => previousState[f] !== state[f])
    if (changes.length) {
      bulk.push({ _action: 'update', _id: key, ...state[key] })
      stats.updated += 1
    } else {
      stats.unmodified += 1
    }
  }
  return { stats, bulk }
}
