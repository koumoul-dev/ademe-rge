// Launch with following command :
// node --max-old-space-size=4096 test-ftp.js

const FTPClient = require('promise-ftp')
const ftp = new FTPClient()
const pump = require('util').promisify(require('pump'))
const { Writable } = require('stream')
const path = require('path')
const fs = require('fs')
const csvStringify = require('csv-stringify/lib/sync')
const moment = require('moment')
const config = require('config')
const Loki = require('lokijs')
const db = new Loki('ademe-rge')
const collection = db.addCollection('entreprises', { indices: ['siret', 'code_qualification', 'organisme', 'traitement_date_fin'] })

const ftpPath = (folder) => `/www/sites/default/files/private/${folder}/archive`
const folders = ['afnor', 'cequami', 'certibat', 'cnoa', 'opqibi', 'qualibat', 'qualifelec', 'qualitenr'] // archives folder empty, or nearly : certivea, icert, lne, opqtecc

const processDayInFolder = require('./process-day-in-folder')
const buildDbFromCsv = require('./build-db-from-csv')

const saveFile = (date) => {
  const filePath = path.join(__dirname, `./data/rge-${date}.csv`)
  console.log('Saving file', filePath)
  const documents = collection.find()
  const header = Object.keys(documents[0]).filter(f => f !== 'meta' && f!== '$loki')
  const formatedDocs = documents.map(doc => {
    const d = JSON.parse(JSON.stringify(doc))
    d.date_debut = d.date_debut + ''
    d.date_fin = d.date_fin + ''
    if (d.date_debut.length !== 8 || d.date_fin.length !== 8) console.log(d.date_debut, d.date_fin)
    d.date_debut = `${d.date_debut.slice(0, 4)}-${d.date_debut.slice(4, 6)}-${d.date_debut.slice(6, 8)}`
    d.date_fin = `${d.date_fin.slice(0, 4)}-${d.date_fin.slice(4, 6)}-${d.date_fin.slice(6, 8)}`
    return d
  })
  fs.writeFileSync(filePath, csvStringify(formatedDocs, { columns: header, header: true, quoted_string: true }))
}

const process = async () => {
  const serverMessage = await ftp.connect(config.ftpOptions)
  console.log('Connected :', serverMessage)
  const days = {}
  for (const folder of folders) {
    console.log('Listing files in', folder)
    const files = await ftp.list(ftpPath(folder))
    const folderDays = Array.from(new Set(files.map(f => f.name.split('-').shift())))
    for (const day of folderDays) {
      days[day] = days[day] || []
      days[day].push(folder)
    }
  }
  let daysList = Object.keys(days)
  daysList.sort()
  console.log(daysList.length, 'days from', daysList[0], 'to', daysList[daysList.length - 1])
  const { lastProcessedDay, errorsStream } = await buildDbFromCsv(collection)
  if (lastProcessedDay) {
    const idx = daysList.findIndex(d => d === lastProcessedDay) + 1
    console.log('Skipping', idx, 'days')
    daysList = daysList.slice(idx)
  }
  let cpt = 0
  for (const day of daysList) {
    const unclosedRecords = collection.find({ traitement_date_fin: undefined })
    const unprocessedRecords = Object.assign({}, ...unclosedRecords.map(d => ({[d.$loki]: d})))
    console.log(`${moment().format('LTS')} ${unclosedRecords.length} unclosed records`)

    for (const folder of days[day]) {
      try {
        console.log(moment().format('LTS'), 'Getting files for date', day, 'and folder', folder)
        const files = {}
        for (const file of ['qualifications', 'entreprises', 'liens']) {
          console.log(moment().format('LTS'), 'Getting file', ftpPath(folder) + `/${day}-${file}.csv`)
          const readableStream = await ftp.get(ftpPath(folder) + `/${day}-${file}.csv`)
          let buffer = Buffer.alloc(0)
          await pump(readableStream, new Writable({
            write(chunk, encoding, callback) {
              // console.log('chunk', chunk)
              buffer = Buffer.concat([buffer, chunk])
              callback()
            }
          }))
          files[file] = buffer
        }
        console.log(`${moment().format('LTS')} Processing ${day} ${folder}`)
        processDayInFolder(files, collection, day, folder, unprocessedRecords, errorsStream)
      } catch (err) {
        errorsStream.write(`${day} ${folder} - Error processing files for date ${day} and folder ${folder} : ${err}\n`)
      }
    }
    console.log(`${moment().format('LTS')} Closing ${Object.keys(unprocessedRecords).length} unprocessed records`)
    const dateDay = moment(day, 'YYYYMMDD')
    const prevDay = dateDay.add(-1, 'days').format('YYYYMMDD')
    Object.values(unprocessedRecords).forEach(record => {
      record.traitement_date_fin = prevDay
      record.date_fin = Number(prevDay)
      collection.update(record)
    })

    cpt++
    if (cpt % 20 === 0) saveFile(day)
    console.log(moment().format('LTS'), cpt, '- Number of docs', collection.count())
  }
  await ftp.end()
  errorsStream.end()
  saveFile('full')
}

process()
