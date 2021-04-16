const parse = require('csv-parse/lib/sync')
const parserOpts = { delimiter: ',' }

const path = require('path')
const fs = require('fs')

const statsHeader = ['day', 'folder', 'dayRecords', 'unmodified', 'inserted', 'dateUpdated', 'changed', 'closed']

module.exports = async function (tmpDir, collection) {
  let lastDate
  try {
    lastDate = fs.readFileSync(path.join(tmpDir, 'rge-processing-date'), 'utf-8')
  } catch (err) {
    console.log('No files to restore from')
    const statsStream = fs.createWriteStream(path.join(tmpDir, 'stats.csv'))
    statsStream.write(statsHeader.map(h => `"${h}"`).join(',') + '\n')
    return {
      lastProcessedDay: null,
      infosStream: fs.createWriteStream(path.join(tmpDir, 'infos.log')),
      statsStream
    }
  }
  console.log('Restoring from date', lastDate)
  const lines = parse(fs.readFileSync(path.join(tmpDir, 'rge-open.csv')), parserOpts)
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
    statsStream: fs.createWriteStream(path.join(tmpDir, 'stats.csv'), { flags: 'a' }),
    infosStream: fs.createWriteStream(path.join(tmpDir, 'infos.log'), { flags: 'a' })
  }
}
