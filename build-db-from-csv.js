const parse = require('csv-parse/lib/sync')
const parserOpts = { delimiter: ',' }

const path = require('path')
const fs = require('fs')
const { promisify } = require('util')
const readdir = promisify(fs.readdir)

const statsHeader = ['day', 'folder', 'dayRecords', 'unmodified', 'inserted', 'dateUpdated', 'changed', 'closed']

module.exports = async function(collection) {
  let lastDate
  try{
    lastDate = fs.readFileSync(path.join(__dirname, './data/rge-processing-date'), 'utf-8')
  }  catch(err){
    console.log('No files to restore from')
    const statsStream = fs.createWriteStream(path.join(__dirname, './data/stats.csv'))
    statsStream.write(statsHeader.map(h => `"${h}"`).join(',')+'\n')
    return {
      lastProcessedDay: null,
      errorsStream: fs.createWriteStream(path.join(__dirname, './log/errors.log')),
      infosStream: fs.createWriteStream(path.join(__dirname, './log/infos.log')),
      statsStream
    }
  }
  console.log('Restoring from date', lastDate)
  const lines = parse(fs.readFileSync(path.join(__dirname, './data/rge-open.csv')), parserOpts)
  const header = lines.shift()
  lines.forEach(line => {
    const data = Object.assign({}, ...header.map((h, i) => ({ [h]: line[i] })))
    data.date_debut = Number(data.date_debut.slice(0, 4) + data.date_debut.slice(5, 7) + data.date_debut.slice(8, 10))
    data.date_fin = Number(data.date_fin.slice(0, 4) + data.date_fin.slice(5, 7) + data.date_fin.slice(8, 10))
    data.particulier = data.particulier === '1'
    collection.insert(data)
  })
  console.log('Done restoring')
  return {
    lastProcessedDay: lastDate,
    errorsStream: fs.createWriteStream(path.join(__dirname, './log/errors.log'), {flags: 'a'}),
    statsStream: fs.createWriteStream(path.join(__dirname, './data/stats.csv'), {flags: 'a'}),
    infosStream: fs.createWriteStream(path.join(__dirname, './log/infos.log'), {flags: 'a'})
  }
}
