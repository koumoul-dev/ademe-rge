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

const { day2int, int2day } = require('./format')

module.exports = async (previousState, state, previousDay, day, historyData) => {
  const bulk = []
  const stats = { closed: 0, created: 0, unmodified: 0, updated: 0 }
  for (const key in previousState) {
    if (!state[key]) {
      // disappeared in current state, close record and do not open a new one

      const historyRecord = historyData.map[key].records[historyData.map[key].records.length - 1]
      historyRecord.traitement_date_fin = day2int(previousDay)
      bulk.push({
        _action: 'patch',
        _id: key + '-' + int2day(historyRecord.traitement_date_debut),
        traitement_termine: true,
        traitement_date_fin: previousDay,
        date_fin: previousDay
      })
      stats.closed += 1
    }
  }
  for (const key in state) {
    if (!previousState[key]) {
      // appeared in current state, create new record

      historyData.map[key] = historyData.map[key] || { records: [] }
      if (historyData.map[key].records.length) {
        const historyRecord = historyData.map[key].records[historyData.map[key].records.length - 1]
        // check inconsistent date_debut (might have been closed in the mean time)
        if (historyRecord.traitement_date_fin > day2int(state[key].date_debut)) {
          state[key].date_debut = day
        }
      }
      historyData.map[key].records.push({ traitement_date_debut: day2int(day), date_debut: day2int(state[key].date_debut) })
      bulk.push({
        _action: 'create',
        _id: key + '-' + day,
        ...state[key],
        traitement_termine: false,
        traitement_date_debut: day,
        date_debut: state[key].date_debut
      })
      stats.created += 1
      continue
    }

    const historyRecord = historyData.map[key].records[historyData.map[key].records.length - 1]
    const changes = checkFields.filter(f => previousState[f] !== state[f])
    if (changes.length) {
      // changes on a key that means we have to close / open a new record

      historyRecord.traitement_date_fin = day2int(previousDay)
      bulk.push({
        _action: 'patch',
        _id: key + '-' + int2day(historyRecord.traitement_date_debut),
        traitement_termine: true,
        traitement_date_fin: previousDay,
        date_fin: previousDay
      })
      stats.closed += 1

      historyData.map[key].records.push({ traitement_date_debut: day2int(day), date_debut: day2int(day) })
      bulk.push({
        _action: 'create',
        _id: key + '-' + day,
        ...state[key],
        traitement_termine: false,
        traitement_date_debut: day,
        date_debut: day
      })
      stats.created += 1
    } else {
      if (state[key].date_fin !== previousState[key].date_fin) {
        // no significant change except for date_fin, patch it only
        bulk.push({
          _action: 'patch',
          _id: key + '-' + int2day(historyRecord.traitement_date_debut),
          date_fin: state[key].date_fin
        })
        stats.updated += 1
      } else {
        stats.unmodified += 1
      }
    }
  }
  return { stats, bulk }
}
