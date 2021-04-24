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
const moment = require('moment')

module.exports = async (previousState, state, previousDay, day, historyData) => {
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
    if (!previousState[key]) {
      // appeared in current state, create new record
      state[key].date_debut = day
      historyData.map[key] = day2int(state[key].date_debut)
      bulk.push({
        _action: 'create',
        _id: key + '-' + day,
        ...state[key],
        traitement_termine: false
      })
      stats.created += 1
      continue
    }

    const changes = checkFields.filter(f => previousState[key][f] !== state[key][f])
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
        ...state[key],
        traitement_termine: false,
        date_debut: day,
        motif_insertion: changes.join(';')
      })
      stats.created += 1
    } else {
      if (state[key].date_fin !== previousState[key].date_fin) {
        // no significant change except for date_fin, patch it only
        bulk.push({
          _action: 'patch',
          _id: key + '-' + int2day(historyData.map[key]),
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
