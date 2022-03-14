const pump = require('util').promisify(require('pump'))
const Iconv = require('iconv').Iconv
const iconv = new Iconv('latin1', 'utf-8')
const parse = require('csv-parse/lib/sync')
const parserOpts = { delimiter: ';', quote: '', record_delimiter: '\r\n' }

const path = require('path')
const fs = require('fs-extra')

const { day2date, parseNumber } = require('./format')

// read the 3 files 'entreprises', 'qualifications' and 'liens' and build an object with a daily state
exports.readDailyState = async (ftp, dir, folder, day, qualifDomaine, ftpBasepath, log) => {
  const files = {}
  await fs.ensureDir(path.join(dir, folder, day))
  for (const file of ['qualifications', 'entreprises', 'liens']) {
    const filePath = path.join(dir, folder, day, `${file}.csv`)
    if (!await fs.pathExists(filePath)) {
      const ftpFilePath = path.join(ftpBasepath, folder, `/archive/${day.replace(/-/g, '')}-${file}.csv`)

      await log.debug('téléchargement du fichier ' + ftpFilePath)
      // creating empty file before streaming seems to fix some weird bugs with NFS
      await fs.ensureFile(filePath + '.tmp')
      await pump(await ftp.get(ftpFilePath), fs.createWriteStream(filePath + '.tmp'))
      // Try to prevent weird bug with NFS by forcing syncing file before reading it
      const fd = await fs.open(filePath + '.tmp', 'r')
      await fs.fsync(fd)
      await fs.close(fd)

      await fs.move(filePath + '.tmp', filePath)
    } else {
      await log.debug('lecture du fichier précédemment téléchargé ' + path.join(folder, day, `${file}.csv`))
    }
    files[file] = await fs.readFile(filePath)
  }
  const state = {}
  const qualificationsLines = parse(iconv.convert(files.qualifications), parserOpts)
  const qualifications = qualificationsLines.reduce((a, q) => { a[q[0]] = q[1]; return a }, {})
  const entreprisesLines = parse(iconv.convert(files.entreprises), parserOpts)
  const entreprises = entreprisesLines.reduce((a, e) => { a[e[0]] = e; return a }, {})
  const liens = parse(iconv.convert(files.liens), parserOpts)
  const entreprisesMiss = new Set()
  const domainesMiss = new Set()
  const qualifMiss = new Set()
  for (const lien of liens) {
    const entreprise = entreprises[lien[0]]
    if (!entreprise) {
      entreprisesMiss.add(lien[0])
      continue
    }
    const domaine = qualifDomaine[lien[1]]
    if (!domaine) {
      domainesMiss.add(lien[1])
    }
    const qualification = qualifications[lien[1]]
    if (!qualification) {
      qualifMiss.add(lien[1])
      continue
    }

    const data = {
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
      nom_qualification: qualification + ` (${lien[1]})`,
      date_debut: lien[2] ? day2date(lien[2]) : undefined,
      date_fin: lien[3] ? day2date(lien[3]) : undefined,
      url_qualification: lien[4],
      nom_certificat: lien[5],
      organisme: domaine ? (domaine.ORGANISME || 'Non renseigné') : 'Inconnu',
      domaine: domaine ? (domaine.DOMAINE_TRAVAUX || 'Non renseigné') : 'Inconnu',
      meta_domaine: domaine ? (domaine.TITRE_DOMAINE || 'Non renseigné') : 'Inconnu',
      particulier: lien[6] === '1'
    }
    data.lien_date_debut = data.date_debut
    data.lien_date_fin = data.date_fin
    const entreprise_id_organisme = entreprise[0]
    state[`${entreprise_id_organisme}-${data.code_qualification}`] = data
  }

  if (entreprisesMiss.size) log.error(`${day} - entreprises manquantes : ${[...entreprisesMiss].join(', ')}`)
  if (domainesMiss.size) log.error(`${day} - domaines manquants : ${[...domainesMiss].join(', ')}`)
  if (qualifMiss.size) log.error(`${day} - qualifications manquantes : ${[...qualifMiss].join(', ')}`)
  return state
}

exports.clearFiles = async (dir, folder, day, log) => {
  await log.debug(`suppression des fichiers d'état ${path.join(folder, day)}`)
  await fs.remove(path.join(dir, folder, day))
}
