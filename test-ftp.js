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
// const collection = db.addCollection('entreprises', {transactional: false} )
const collection = db.addCollection('entreprises', { indices: ['siret', 'code_qualification'] })

const ftpPath = (folder) => `/www/sites/default/files/private/${folder}/archive`
const folders = ['afnor', 'cequami', 'certibat', 'cnoa', 'opqibi', 'qualibat', 'qualifelec', 'qualitenr'] // archives folder empty, or nearly : certivea, icert, lne, opqtecc

const processDayInFolder = require('./process-day-in-folder')
const buildDbFromCsv = require('./build-db-from-csv')

const save = (date, temporary, writeHeader) => {
  const filePath = path.join(__dirname, './data/rge-open.csv')
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
  const archivedFile = path.join(__dirname, './data/rge-archived.csv')
  if(temporary) {
    console.log('Saving temporary files for date', date)
    const current = formatedDocs.filter(d => !d.traitement_date_fin || !d.traitement_date_fin.length)
    const archived = formatedDocs.filter(d => d.traitement_date_fin && d.traitement_date_fin.length)
    fs.writeFileSync(filePath, csvStringify(current, { columns: header, header: true, quoted_string: true }))
    fs.writeFileSync(archivedFile, csvStringify(archived, { columns: header, header: writeHeader, quoted_string: true }), {flag: 'a'})
    archived.forEach(doc => {
      collection.remove(doc)
    })
    fs.writeFileSync(path.join(__dirname, './data/rge-processing-date'), date)
  } else{
    console.log('Saving file for date', date)
    const outFile = path.join(__dirname, `./data/rge-${date}.csv`)
    if(fs.existsSync(archivedFile)){
      fs.copyFileSync(archivedFile, outFile)
      fs.writeFileSync(outFile, csvStringify(formatedDocs, { columns: header, header: false, quoted_string: true }), {flag: 'a'})
    }else{
      fs.writeFileSync(outFile, csvStringify(formatedDocs, { columns: header, header: true, quoted_string: true }))
    }
  }
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
  const { lastProcessedDay, errorsStream, statsStream } = await buildDbFromCsv(collection)
  let cpt = 0
  if (lastProcessedDay) {
    cpt = daysList.findIndex(d => d === lastProcessedDay) + 1
    console.log('Skipping', cpt, 'days')
    daysList = daysList.slice(cpt)
  }
  for (const day of daysList) {
    const unclosedRecords = collection.find()
    console.log(`${moment().format('LTS')} ${unclosedRecords.length} unclosed records`)
    const unprocessedRecords = {} //Object.assign({}, ...unclosedRecords.map(d => ({[d.$loki]: d})))
    for (const folder of days[day]) {
      unclosedRecords.filter(r => r.folder === folder).forEach(d => {
        unprocessedRecords[d.$loki] = d
      })
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
        processDayInFolder(files, collection, day, folder, unprocessedRecords, errorsStream, statsStream)
      } catch (err) {
        errorsStream.write(`${day} ${folder} - Error processing files for date ${day} and folder ${folder} : ${err}\n`)
      }
    }
    console.log(`${moment().format('LTS')} **** Closing ${Object.keys(unprocessedRecords).length} unprocessed records ****`)
    const dateDay = moment(day, 'YYYYMMDD')
    const prevDay = dateDay.add(-1, 'days').format('YYYYMMDD')
    Object.values(unprocessedRecords).forEach(record => {
      record.traitement_date_fin = prevDay
      record.date_fin = Number(prevDay)
      collection.update(record)
    })

    cpt++
    if(cpt % 100 === 0) save(day)
    save(day, true, !lastProcessedDay && cpt === 1)
    console.log(moment().format('LTS'), cpt, '- Number of docs', collection.count())
  }
  await ftp.end()
  errorsStream.end()
  statsStream.end()
  save('full')
}

process()
