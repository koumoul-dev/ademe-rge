const FTPClient = require('promise-ftp')
const ftp = new FTPClient()
const pump = require('util').promisify(require('pump'))
const { Writable } = require('stream')
const path = require('path')
const fs = require('fs')
const csvStringify = require('csv-stringify/lib/sync')
const moment = require('moment')
const Loki = require('lokijs')
const db = new Loki('ademe-rge')
// const collection = db.addCollection('entreprises', {transactional: false} )
const collection = db.addCollection('entreprises', { indices: ['siret', 'code_qualification'] })

const ftpPath = (folder) => `/www/sites/default/files/private/${folder}/archive`

const processDayInFolder = require('./process-day-in-folder')
const buildDbFromCsv = require('./build-db-from-csv')

const save = (tmpDir, date, temporary, writeHeader) => {
  const filePath = path.join(tmpDir, 'rge-open.csv')
  const documents = collection.find()
  if (documents.length) {
    const header = Object.keys(documents[0]).filter(f => f !== 'meta' && f !== '$loki')
    const formatedDocs = documents.map(doc => {
      const d = JSON.parse(JSON.stringify(doc))
      d.date_debut = d.date_debut + ''
      d.date_fin = (d.date_fin || '') + '' // It seems it can be null in some files ...
      if (d.date_debut.length !== 8 || d.date_fin.length !== 8) console.log(date, 'Error for dates length', d.date_debut, d.date_fin)
      d.date_debut = `${d.date_debut.slice(0, 4)}-${d.date_debut.slice(4, 6)}-${d.date_debut.slice(6, 8)}`
      if (d.date_fin.length === 8) d.date_fin = `${d.date_fin.slice(0, 4)}-${d.date_fin.slice(4, 6)}-${d.date_fin.slice(6, 8)}`
      return d
    })
    const archivedFile = path.join(tmpDir, 'rge-archived.csv')
    if (temporary) {
      console.log('Saving temporary files for date', date)
      const current = formatedDocs.filter(d => !d.traitement_date_fin || !d.traitement_date_fin.length)
      const archived = formatedDocs.filter(d => d.traitement_date_fin && d.traitement_date_fin.length)
      fs.writeFileSync(filePath, csvStringify(current, { columns: header, header: true, quoted_string: true }))
      fs.writeFileSync(archivedFile, csvStringify(archived, { columns: header, header: writeHeader, quoted_string: true }), { flag: 'a' })
      archived.forEach(doc => {
        collection.remove(doc)
      })
      fs.writeFileSync(path.join(tmpDir, 'rge-processing-date'), date)
    } else {
      console.log('Saving file for date', date)
      const outFile = path.join(tmpDir, `rge-${date}.csv`)
      if (fs.existsSync(archivedFile)) {
        fs.copyFileSync(archivedFile, outFile)
        fs.writeFileSync(outFile, csvStringify(formatedDocs, { columns: header, header: false, quoted_string: true }), { flag: 'a' })
      } else {
        fs.writeFileSync(outFile, csvStringify(formatedDocs, { columns: header, header: true, quoted_string: true }))
      }
    }
  }
}

module.exports = async (tmpDir, ftpOptions, folders, maxDays, log) => {
  await log.step('Connexion au serveur FTP')
  const serverMessage = await ftp.connect(ftpOptions)
  await log.info('connecté : ' + serverMessage)
  await log.step('Préparation du traitement')
  const days = {}
  for (const folder of folders) {
    await log.info('récupération de la liste des fichiers dans le répertoire ' + folder)
    const files = await ftp.list(ftpPath(folder))
    const folderDays = Array.from(new Set(files.map(f => f.name.split('-').shift()).filter(f => f.length === 8 && !f.includes('.'))))
    for (const day of folderDays) {
      days[day] = days[day] || []
      days[day].push(folder)
    }
  }
  let daysList = Object.keys(days)
  daysList.sort()
  await log.info(`interval de jours dans la donnée : début = ${daysList[0]}, fin = ${daysList[daysList.length - 1]}`)
  const { lastProcessedDay, statsStream, infosStream } = await buildDbFromCsv(tmpDir, collection)
  let cpt = 0
  if (lastProcessedDay) {
    cpt = daysList.findIndex(d => d === lastProcessedDay) + 1
    await log.info(`nombre de jours déjà traités : ${cpt}`)
    daysList = daysList.slice(cpt)
  }
  if (maxDays !== -1 && daysList.length > maxDays) {
    daysList = daysList.slice(0, maxDays)
    await log.info(`nombre de jours à traiter restreint par la configuration : ${daysList.length}`)
  }
  await log.step('Exécution du traitement jour par jour')
  for (const day of daysList) {
    await log.info(`jour ${day}`)
    const unclosedRecords = collection.find()
    await log.info(`enregistrements ouverts précédemment : ${unclosedRecords.length}`)
    const unprocessedRecords = {} // Object.assign({}, ...unclosedRecords.map(d => ({[d.$loki]: d})))
    for (const folder of days[day]) {
      unclosedRecords.filter(r => r.folder === folder).forEach(d => {
        unprocessedRecords[d.$loki] = d
      })
      try {
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
        processDayInFolder(files, collection, day, folder, unprocessedRecords, statsStream, infosStream)
      } catch (err) {
        log.error(`échec du traitement jour=${day}, répertoire=${folder} : ${err}`)
      }
    }
    await log.info(`fermeture de ${Object.keys(unprocessedRecords).length} enregistrements absents de la dernière journée`)
    const dateDay = moment(day, 'YYYYMMDD')
    const prevDay = dateDay.add(-1, 'days').format('YYYYMMDD')
    Object.values(unprocessedRecords).forEach(record => {
      record.traitement_date_fin = prevDay
      record.date_fin = Number(prevDay)
      // if(record.entreprise_id_organisme === '64') infosStream.write(`${day} ${record.folder} - Close : ${JSON.stringify(record, null, 2)}\n`)
      collection.update(record)
    })

    cpt++
    if (cpt % 100 === 0) save(day)
    save(tmpDir, day, true, !lastProcessedDay && cpt === 1)
    console.log(moment().format('LTS'), cpt, '- Number of docs', collection.count())
  }
  await ftp.end()
  statsStream.end()
  infosStream.end()
  save('full')
}
