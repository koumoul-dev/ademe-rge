const fs = require('fs-extra')
const path = require('path')
const pump = require('util').promisify(require('pump'))
const Iconv = require('iconv').Iconv
const iconv = new Iconv('latin1', 'utf-8')
const parse = require('csv-parse/lib/sync')
const parserOpts = { delimiter: ';', quote: '', record_delimiter: '\r\n' }
const ftpPath = '/www/sites/default/files/private/'

const { parseNumber, date2day } = require('./format')

const validateFormat = async (filePath, nbCols, validateLine, context, prefix) => {
  const errors = []
  // TODO: a way of checking ANSI/windows-1252 encoding ?
  try {
    const lines = parse(iconv.convert(await fs.readFile(filePath)), parserOpts)
    const badNbCols = []
    const badChars = []
    for (const i in lines) {
      const line = lines[i]
      if (line.length !== nbCols) {
        badNbCols.push(i + 1)
      } else {
        for (const item of line) {
          if (item.includes(';') || item.includes('\n') || item.includes('"')) badChars.push(i + 1)
        }
        validateLine(line, i, errors, context)
      }
    }
    if (badNbCols.length) {
      errors.unshift(`${prefix}${badNbCols.length}/${lines.length} lignes n'ont pas les ${nbCols} éléments attendus (lignes ${badNbCols.length > 5 ? badNbCols.slice(0, 5).join(', ') + ', ...' : badNbCols.join(', ')})`)
    }
    if (badChars.length) {
      errors.unshift(`${prefix}${badChars.length}/${lines.length} lignes contiennents des caractères interdits (lignes ${badChars.length > 5 ? badChars.slice(0, 5).join(', ') + ', ...' : badChars.join(', ')})`)
    }
  } catch (err) {
    errors.unshift(err.message)
  }
  return errors
}

const validateEntrepriseLine = (line, i, errors, context) => {
  const prefix = `entreprises.csv ligne ${i + 1} - `
  // entreprise_id_organisme
  if (!line[0]) errors.push(prefix + 'le champ "entreprise_id_organisme" est obligatoire')
  else {
    context.idEntreprises.add(line[0])
    if (!line[0].length > 25) errors.push(prefix + 'le champ "entreprise_id_organisme" dépasse la limite de 25 caractères')
    if (!line[0].match(/^[a-zA-Z0-9]*$/)) errors.push(prefix + 'le champ "entreprise_id_organisme" contient des caractères invalides - ' + line[0])
  }

  // siret
  if (!line[1]) errors.push(prefix + 'le champ "siret" est obligatoire')
  else if (line[1].length !== 14) errors.push(prefix + 'le champ "siret" doit faire exactement 14 caractères - ' + line[1])
  // nom de l'entreprise
  if (!line[2]) errors.push(prefix + 'le champ "nom de l\'entreprise" est obligatoire')
  else if (line[2].length > 100) errors.push(prefix + 'le champ "nom de l\'entreprise" dépasse la limite de 100 caractères')
  // adresse ligne 1
  if (!line[3]) errors.push(prefix + 'le champ "adresse ligne 1" est obligatoire')
  else if (line[3].length > 150) errors.push(prefix + 'le champ "adresse ligne 1" dépasse la limite de 150 caractères')
  // adresse ligne 2
  if (line[4].length > 150) errors.push(prefix + 'le champ "adresse ligne 2" dépasse la limite de 150 caractères')
  // adresse ligne 3
  if (line[5].length > 150) errors.push(prefix + 'le champ "adresse ligne 3" dépasse la limite de 150 caractères')
  // code postal
  if (!line[6]) errors.push(prefix + 'le champ "code postal" est obligatoire')
  else if (!line[6].match(/^[0-9]{5}$/)) errors.push(prefix + 'le champ "code postal" doit contenir exactement 5 chiffres - ' + line[6])
  // ville
  if (!line[7]) errors.push(prefix + 'le champ "ville" est obligatoire')
  else if (line[7].length > 55) errors.push(prefix + 'le champ "ville" dépasse la limite de 55 caractères')
  // latitude
  if (line[8]) {
    const latitude = parseNumber(line[8])
    if (isNaN(latitude)) errors.push(prefix + 'le champ "latitude" n\'est pas un nombre valide')
    else if (latitude < -90 || latitude > 90) errors.push(prefix + 'le champ "latitude" n\'est pas compris entre -90 et 90 - ' + line[8])
  }
  // longitude
  if (line[9]) {
    const longitude = parseNumber(line[9])
    if (isNaN(longitude)) errors.push(prefix + 'le champ "longitude" n\'est pas un nombre valide')
    else if (longitude < -180 || longitude > 180) errors.push(prefix + 'le champ "longitude" n\'est pas compris entre -90 et 90 - ' + line[9])
  }
  // téléphone
  if (line[10]) {
    if (!line[10].match(/^[0-9]{2} [0-9]{2} [0-9]{2} [0-9]{2} [0-9]{2}$/)) {
      errors.push(prefix + 'le champ "téléphone" n\'a pas le format attendu - ' + line[10])
    }
  }
  // email
  if (line[11].length > 110) errors.push(prefix + 'le champ "email" dépasse la limite de 110 caractères')
  // site internet
  if (line[12].length > 255) errors.push(prefix + 'le champ "site internet" dépasse la limite de 255 caractères')
  // TODO: cette règle n'est peut-être pas obligatoire, la demande est peu précise "nous vous demandons de bien faire attention..."
  if (line[12] && !line[12].startsWith('http://') && !line[12].startsWith('https://')) {
    errors.push(prefix + 'le champ "site internet" doit commencer par http:// ou https:// - ' + line[12])
  }
}

const validateQualificationsLine = (line, i, errors, context) => {
  const prefix = `qualifications.csv ligne ${i + 1} - `
  // code
  if (!line[0]) errors.push(prefix + 'le champ "code" est obligatoire')
  else {
    context.codeQualifs.add(line[0])
    if (!line[0].match(/^[A-Za-z0-9]{0,10}$/)) errors.push(prefix + 'le champ "code" n\'a pas le format attendu - ' + line[0])
  }
  // nom de la qualification
  if (!line[1]) errors.push(prefix + 'le champ "nom de la qualification" est obligatoire')
  else if (!line[1].length > 255) errors.push(prefix + 'le champ "nom de la qualification" dépasse la limite de 255 caractères')
}

const validateLiensLine = (line, i, errors, context) => {
  const prefix = `liens.csv ligne ${i + 1} - `
  // entreprise_id_organisme
  if (!line[0]) errors.push(prefix + 'le champ "entreprise_id_organisme" est obligatoire')
  else if (!context.idEntreprises.has(line[0])) errors.push(prefix + 'le champ "entreprise_id_organisme" ne correspond pas à une ligne de entreprises.csv - ' + line[0])
  // qualification code
  if (!line[1]) errors.push(prefix + 'le champ "qualification code" est obligatoire')
  else if (!context.codeQualifs.has(line[1])) errors.push(prefix + 'le champ "qualification code" ne correspond pas à une ligne de qualifications.csv - ' + line[0])
  // date début
  if (!line[2]) errors.push(prefix + 'le champ "date début" est obligatoire')
  else if (!line[2].match(/^[0-9]{8}$/)) errors.push(prefix + 'le champ "date début" n\'a pas le format attendu - ' + line[2])
  // date fin
  if (!line[3]) errors.push(prefix + 'le champ "date fin" est obligatoire')
  else if (!line[3].match(/^[0-9]{8}$/)) errors.push(prefix + 'le champ "date fin" n\'a pas le format attendu - ' + line[2])
  // url qualification
  if (!line[4]) errors.push(prefix + 'le champ "url qualification" est obligatoire')
  else if (line[4].length > 255) errors.push(prefix + 'le champ "url qualification" dépasse la limite de 255 caractères')
  // TODO: cette règle n'est peut-être pas obligatoire, la demande est peu précise "nous vous demandons de bien faire attention..."
  else if (!line[4].startsWith('http://') && !line[4].startsWith('https://')) {
    errors.push(prefix + 'le champ "url qualification" doit commencer par http:// ou https:// - ' + line[4])
  }
  // libellé certificat
  if (!line[5]) errors.push(prefix + 'le champ "libellé certificat" est obligatoire')
  else if (line[5].length > 255) errors.push(prefix + 'le champ "libellé certificat" dépasse la limite de 255 caractères')
  // particulier
  if (!['0', '1'].includes(line[6])) errors.push('le champ "particulier" n\'a pas le format attendu - ' + line[6])
}

// les fichiers déposés par les organismes sont validés puis déplacés dans ./archive
exports.downloadAndValidate = async (ftp, dir, folder, files, log) => {
  // this function logs errors and returns them in an array, ready to be sent to a contact
  let errors = []

  const importFolder = path.join(dir, folder, 'import')
  await fs.emptyDir(importFolder)

  for (const file of files) {
    await log.info('téléchargement du fichier ' + file)
    const filePath = path.join(importFolder, file)
    // creating empty file before streaming seems to fix some weird bugs with NFS
    await fs.ensureFile(filePath + '.tmp')
    await pump(await ftp.get(path.join(ftpPath, folder, file)), fs.createWriteStream(filePath + '.tmp'))
    // Try to prevent weird bug with NFS by forcing syncing file before reading it
    const fd = await fs.open(filePath + '.tmp', 'r')
    await fs.fsync(fd)
    await fs.close(fd)
    await fs.move(filePath + '.tmp', filePath)
  }

  const context = { idEntreprises: new Set(), codeQualifs: new Set() }
  if (!files.includes('entreprises.csv')) {
    await errors.push('la livraison ne contient pas de fichier entreprises.csv')
  } else {
    await log.info('vérification du format du fichier entreprises.csv')
    const formatErrors = await validateFormat(path.join(importFolder, 'entreprises.csv'), 13, validateEntrepriseLine, context, 'entreprises.csv - ')
    for (const error of formatErrors) await log.error(error)
    errors = errors.concat(formatErrors)
  }
  if (!files.includes('qualifications.csv')) {
    await errors.push('la livraison ne contient pas de fichier qualifications.csv')
  } else {
    await log.info('vérification du format du fichier qualifications.csv')
    const formatErrors = await validateFormat(path.join(importFolder, 'qualifications.csv'), 2, validateQualificationsLine, context, 'qualifications.csv - ')
    for (const error of formatErrors) await log.error(error)
    errors = errors.concat(formatErrors)
  }
  if (!files.includes('liens.csv')) {
    await errors.push('la livraison ne contient pas de fichier liens.csv')
  } else {
    await log.info('vérification du format du fichier liens.csv')
    const formatErrors = await validateFormat(path.join(importFolder, 'liens.csv'), 7, validateLiensLine, context, 'liens.csv - ')
    for (const error of formatErrors) await log.error(error)
    errors = errors.concat(formatErrors)
  }

  return errors
}

exports.moveToFtp = async (ftp, dir, folder, error, log) => {
  const day = date2day(new Date())
  const importFolder = path.join(dir, folder, 'import')
  const ftpDir = path.join(folder, error ? '/erreur' : '/archive')
  for (const file of ['entreprises', 'qualifications', 'liens']) {
    const filePath = path.join(importFolder, `${file}.csv`)
    if (await fs.pathExists(filePath)) {
      await log.info(`${folder}/${file}.csv -> ` + path.join(ftpDir, `${day}-${file}.csv`))
      await ftp.put(await fs.readFile(filePath), path.join(ftpPath, ftpDir, `${day}-${file}.csv`))
      await ftp.delete(path.join(ftpPath, folder, `${file}.csv`))
    }
  }
  await fs.remove(importFolder)
}

exports.sendValidationErrors = async (folder, contactsOrganismes, errors, log, sendMail) => {
  const contact = contactsOrganismes[folder]
  if (!contact) throw new Error(`Pas de contact trouvé pour l'organisme ${folder}`)
  await log.info(`send mail to ${contact.MAIL_CONTACT} (${contact.NOM_CONTACT})`)
  await sendMail({
    from: 'contact@ademe.gouv.fr',
    to: contact.MAIL_CONTACT,
    subject: `échec de l'import des données RGE organisme ${folder}`,
    text: `Le validation des données a retourné ${errors.length} erreurs. Veuillez trouver le détail en pièce jointe.`,
    attachments: [{ filename: 'erreurs.txt', content: errors.join('\n') }]
  })
}
