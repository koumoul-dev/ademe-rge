const pump = require('util').promisify(require('pump'))
const { Writable } = require('stream')
const Iconv = require('iconv').Iconv
const iconv = new Iconv('latin1', 'utf-8')
const parse = require('csv-parse/lib/sync')
const parserOpts = { delimiter: ';', quote: '' }

const path = require('path')
const fs = require('fs')
const qualifDomaineLines = parse(fs.readFileSync(path.join(__dirname, '../resources/RGE - Lien domaine qualification.csv')))
const qualifDomaine = Object.assign({}, ...qualifDomaineLines.map(e => ({ [e[1]]: e })))

const ftpPath = (folder) => `/www/sites/default/files/private/${folder}/archive`

const formatDay = (day) => day.slice(0, 4) + '-' + day.slice(4, 6) + '-' + day.slice(6, 8)
const parseNumber = (str) => str ? Number(str.replace(',', '.')) : undefined

// read the 3 files 'entreprises', 'qualifications' and 'liens' and build an object with a daily state
module.exports = async (ftp, folder, day, log) => {
  const files = {}
  for (const file of ['qualifications', 'entreprises', 'liens']) {
    const filePath = ftpPath(folder) + `/${day}-${file}.csv`
    await log.debug('téléchargement du fichier ' + filePath)
    const readableStream = await ftp.get(filePath)
    let buffer = Buffer.alloc(0)
    await pump(readableStream, new Writable({
      write (chunk, encoding, callback) {
        // console.log('chunk', chunk)
        buffer = Buffer.concat([buffer, chunk])
        callback()
      }
    }))
    files[file] = buffer
  }
  const state = {}
  const qualificationsLines = parse(iconv.convert(files.qualifications), parserOpts)
  const qualifications = Object.assign({}, ...qualificationsLines.map(q => ({ [q[0]]: q[1] })))
  const entreprisesLines = parse(iconv.convert(files.entreprises), parserOpts)
  const entreprises = Object.assign({}, ...entreprisesLines.map(e => ({ [e[0]]: e })))
  const liens = parse(iconv.convert(files.liens), parserOpts)
  for (const lien of liens) {
    const entreprise = entreprises[lien[0]]
    if (!entreprise) {
      log.error(`${day} - entreprise manquante ${lien[0]}`)
      continue
    }
    const domaine = qualifDomaine[lien[1]]
    if (!domaine) {
      log.error(`${day} - domaine manquant ${lien[1]}`)
      continue
    }
    const qualification = qualifications[lien[1]]
    if (!qualification) {
      log.error(`${day} - qualification manquante ${lien[1]}`)
      continue
    }

    const year = day.slice(0, 4)

    const data = {
      entreprise_id_organisme: entreprise[0],
      siret: entreprise[1].padStart(14, '0'),
      nom_entreprise: entreprise[2],
      adresse: (entreprise[3] + ' ' + entreprise[4] + ' ' + entreprise[5]).trim(),
      code_postal: entreprise[6],
      commune: entreprise[7],
      latitude: parseNumber(entreprise[8]),
      longitude: parseNumber(entreprise[9]),
      telephone: entreprise[10],
      email: entreprise[11],
      site_internet: entreprise[12],
      code_qualification: lien[1],
      nom_qualification: qualifications[lien[1]] + ` (${lien[1]})`,
      date_debut: lien[2] ? formatDay(lien[2]) : undefined,
      date_fin: lien[3] ? formatDay(lien[3]) : undefined,
      url_qualification: lien[4],
      nom_certificat: lien[5],
      organisme: domaine ? domaine[0] : 'Inconnu',
      domaine: domaine ? (year < '2021' ? domaine[4] || domaine[3] : domaine[3]) : 'Inconnu',
      meta_domaine: domaine ? domaine[5] : 'Inconnu',
      particulier: lien[6] === '1'
      // traitement_date_debut: day,
      // traitement_date_fin: undefined,
      // motif_insertion: undefined
    }
    const { siret, code_qualification } = data
    state[`${siret}-${code_qualification}-${folder}`] = data
  }
  return state
}
