const Iconv = require('iconv').Iconv
const iconv = new Iconv('latin1', 'utf-8')
const parse = require('csv-parse/lib/sync')
const parserOpts = { delimiter: ';', quote: '' }
const moment = require('moment')

const path = require('path')
const fs = require('fs')
const qualifDomaineLines = parse(fs.readFileSync(path.join(__dirname, 'RGE - Lien domaine qualification.csv')))
const qualifDomaine = Object.assign({}, ...qualifDomaineLines.map(e => ({ [e[1]]: e })))

module.exports = function(files, collection, day, folder, unprocessedRecords, errorsStream) {
  // const qualificationsData = fs.readFileSync(path.join(directory, folder, day + '-qualifications.csv'))
  const qualificationsLines = parse(iconv.convert(files.qualifications), parserOpts)
  const qualifications = Object.assign({}, ...qualificationsLines.map(q => ({ [q[0]]: q[1] })))
  // const entreprisesData = fs.readFileSync(path.join(directory, folder, day + '-entreprises.csv'))
  const entreprisesLines = parse(iconv.convert(files.entreprises), parserOpts)
  const entreprises = Object.assign({}, ...entreprisesLines.map(e => ({ [e[0]]: e })))
  // const liensData = fs.readFileSync(path.join(directory, folder, day + '-liens.csv'))
  const liens = parse(iconv.convert(files.liens), parserOpts)
  let inserted = 0
  let updated = 0
  liens.forEach(lien => {
    const entreprise = entreprises[lien[0]]
    const qualification = qualifications[lien[1]]
    const domaine = qualifDomaine[lien[1]]
    if (!entreprise || !qualification) errorsStream.write(`${day} ${folder} - Error, data do not match ${entreprise} ${qualification} ${lien} ${domaine}\n`)
    if (!domaine) errorsStream.write(`${day} ${folder} - Error, no domain for code ${lien[1]}\n`)
    const data = {
      entreprise_id_organisme: entreprise[0],
      siret: entreprise[1].padStart(14, '0'),
      nom_entreprise: entreprise[2],
      adresse: (entreprise[3] + ' ' + entreprise[4] + ' ' + entreprise[5]).trim(),
      code_postal: entreprise[6],
      commune: entreprise[7],
      latitude: entreprise[8],
      longitude: entreprise[9],
      telephone: entreprise[10],
      email: entreprise[11],
      site_internet: entreprise[12],
      code_qualification: lien[1],
      nom_qualification: qualifications[lien[1]] + ` (${lien[1]})`,
      date_debut: Number(lien[2]),
      date_fin: Number(lien[3]),
      url_qualification: lien[4],
      nom_certificat: lien[5],
      organisme: domaine ? domaine[0] : 'Inconnu',
      domaine: domaine ? domaine[3] : 'Inconnu',
      particulier: lien[6] === '1',
      traitement_date_debut: day,
      traitement_date_fin: undefined
    }
    const { siret, code_qualification, organisme } = data
    const records = collection.find({ siret, code_qualification, organisme, traitement_date_fin: undefined })
    if (!records.length){
      collection.insert(data)
      inserted++
    }
    else if(records.length >1) {
       errorsStream.write(`${day} ${folder} - Error, ${records.length} records for ${siret}, ${code_qualification}, ${organisme}\n`)
    } else {
      const record = records.shift()
      record.date_fin = data.date_fin
      delete unprocessedRecords[record.$loki]
      collection.update(record)
      updated++
      // let found = false
      // records.filter(r => !r.traitement_date_fin).forEach(record => {
      //   if (record.date_debut <= data.date_debut && record.date_fin >= data.date_fin) {
      //     found = true
      //   } else if ((record.date_debut >= data.date_debut && record.date_debut <= data.date_fin) || (record.date_fin >= data.date_debut && record.date_fin <= data.date_fin)) {
      //     found = true
      //     // console.log('updating record', record.date_debut, data.date_debut, record.date_fin, data.date_fin, record.date_debut === data.date_debut && record.date_fin === data.date_fin)
      //     record.date_debut = Math.min(record.date_debut, data.date_debut)
      //     record.date_fin = Math.max(record.date_fin, data.date_fin)
      //     collection.update(record)
      //   }
      // })
      // if (!found) {
      //   collection.insert(data)
      //   // console.log('no overlap, adding a new record', records, data)
      // }
    }
  })
  console.log(`${moment().format('LTS')} Inserted ${inserted} records, updated ${updated} records`)
}
