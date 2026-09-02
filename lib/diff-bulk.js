const checkFields = [
  'siret',
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
const moment = require('moment')

module.exports = async (previousState, state, day, historyData, processingConfig) => {
  const bulk = []
  const stats = { closed: 0, created: 0, unmodified: 0, updated: 0 }
  const dayMinus1 = moment(day, 'YYYY-MM-DD').add(-1, 'days').format('YYYY-MM-DD')
  for (const key in previousState) {
    if (!state[key]) {
      // disappeared in current state, close record and do not open a new one
      bulk.push({
        _action: 'patch',
        _id: key + '-' + int2day(historyData.map[key]),
        traitement_termine: true,
        date_fin: dayMinus1
      })
      stats.closed += 1
    }
  }
  for (const key in state) {
    const current = state[key]
    const previous = previousState[key]
    if (!previous) {
      // appeared in current state, create new record
      current.date_debut = day
      historyData.map[key] = day2int(day)
      bulk.push({
        _action: 'create',
        _id: key + '-' + day,
        ...current,
        traitement_termine: false
      })
      stats.created += 1
      continue
    }

    const changes = checkFields.filter(f => previous[f] !== current[f])
    if (changes.length) {
      // changes on a key that means we have to close / open a new record
      bulk.push({
        _action: 'patch',
        _id: key + '-' + int2day(historyData.map[key]),
        traitement_termine: true,
        date_fin: dayMinus1
      })
      stats.closed += 1

      historyData.map[key] = day2int(day)
      bulk.push({
        _action: 'create',
        _id: key + '-' + day,
        ...current,
        traitement_termine: false,
        date_debut: day,
        motif_insertion: changes.join(';')
      })
      stats.created += 1
    } else {
      if (current.lien_date_debut !== previous.lien_date_debut || current.lien_date_fin !== previous.lien_date_fin) {
        // no significant change except for date_fin, patch it only
        bulk.push({
          _action: 'patch',
          _id: key + '-' + int2day(historyData.map[key]),
          date_fin: current.date_fin,
          lien_date_debut: current.lien_date_debut,
          lien_date_fin: current.lien_date_fin
        })
        stats.updated += 1
      } else if (processingConfig.forceLinkDatesUpdate) {
        bulk.push({
          _action: 'patch',
          _id: key + '-' + int2day(historyData.map[key]),
          lien_date_debut: current.lien_date_debut,
          lien_date_fin: current.lien_date_fin,
          code_postal: current.code_postal
        })
        stats.updated += 1
      } else {
        stats.unmodified += 1
      }
    }
  }
  return { stats, bulk }
}
