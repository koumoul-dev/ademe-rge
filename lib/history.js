const fs = require('fs-extra')
const path = require('path')
const Pbf = require('pbf')
const { HistoryData } = require('../resources/history-proto')

exports.readHistoryData = async (dir, folder) => {
  const pbfPath = path.join(dir, folder, 'history.pbf')
  const historyData = await fs.exists(pbfPath)
    ? HistoryData.read(new Pbf(await fs.readFile(pbfPath)))
    : { map: {} }

  historyData.write = async () => {
    const pbf = new Pbf()
    HistoryData.write(historyData, pbf)
    await fs.writeFile(pbfPath, pbf.finish())
  }

  return historyData
}
