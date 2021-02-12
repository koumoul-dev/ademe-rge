const parse = require('csv-parse/lib/sync')
const parserOpts = { delimiter: ',' }

const path = require('path')
const fs = require('fs')
const { promisify } = require('util')
const readdir = promisify(fs.readdir)

module.exports = async function(collection) {
  const files = (await readdir(path.join(__dirname, './data/'))).filter(f => f.includes('rge-') && f.length === 16)

  const options = {}
  const lastFile = files.pop()
  if (!lastFile) {
    console.log('No file to restore from')
    return { lastProcessedDay: null, errorsStream: fs.createWriteStream('errors.log') }
  }
  console.log('Restoring from file', lastFile)

  const lines = parse(fs.readFileSync(path.join(__dirname, './data/', lastFile)), parserOpts)
  const header = lines.shift()
  lines.forEach(line => {
    const data = Object.assign({}, ...header.map((h, i) => ({ [h]: line[i] })))
    data.date_debut = Number(data.date_debut.slice(0, 4) + data.date_debut.slice(5, 7) + data.date_debut.slice(8, 10))
    data.date_fin = Number(data.date_fin.slice(0, 4) + data.date_fin.slice(5, 7) + data.date_fin.slice(8, 10))
    data.particulier = data.particulier === '1'
    if(data.traitement_date_fin === '') data.traitement_date_fin = undefined
    collection.insert(data)
  })
  console.log('Done restoring')
  return { lastProcessedDay: lastFile.slice(4, 12), errorsStream: fs.createWriteStream('errors.log', {flags: 'a'}) }
}
