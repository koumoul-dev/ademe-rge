// tell data-fair-processings to persist the data directory for this processing
exports.preserveDir = true

// main execution method
exports.run = async ({ pluginConfig, processingConfig, processingId, dir, axios, log, patchConfig }) => {
  const fs = require('fs-extra')
  const path = require('path')

  const datasetSchema = require('./resources/schema.json')
  let dataset

  if (processingConfig.datasetMode === 'create') {
    await log.step('Création du jeu de données')
    dataset = (await axios.post('api/v1/datasets', {
      id: processingConfig.dataset.id,
      title: processingConfig.dataset.title,
      isRest: true,
      schema: datasetSchema,
      extras: { processingId }
    })).data
    await log.info(`jeu de donnée créé, id="${dataset.id}", title="${dataset.title}"`)
    await patchConfig({ datasetMode: 'update', dataset: { id: dataset.id, title: dataset.title } })
  } else if (processingConfig.datasetMode === 'update') {
    await log.step('Vérification du jeu de données')
    dataset = (await axios.get(`api/v1/datasets/${processingConfig.dataset.id}`)).data
    if (!dataset) throw new Error(`le jeu de données n'existe pas, id${processingConfig.dataset.id}`)
    await log.info(`le jeu de donnée existe, id="${dataset.id}", title="${dataset.title}"`)
  }

  await log.step('Connexion au serveur FTP')
  const FTPClient = require('promise-ftp')
  const ftp = new FTPClient()
  const serverMessage = await ftp.connect({
    ftpOptions: {
      host: 'localhost',
      port: 21,
      user: undefined,
      password: undefined,
      connTimeout: 30000,
      pasvTimeout: 30000,
      keepalive: 30000,
      autoReconnect: true
    },
    ...pluginConfig.ftpOptions
  })
  await log.info('connecté : ' + serverMessage)

  const { day2int, int2day, day2date } = require('./lib/format')
  const { readHistoryData } = require('./lib/history')
  const { readDailyState, clearFiles } = require('./lib/daily-state')

  for (const folder of processingConfig.folders) {
    await fs.ensureDir(path.join(dir, folder))
    log.step(`Traitement du répertoire ${folder}`)
    await log.info('récupération de la liste des fichiers dans le répertoire ' + folder)
    const files = await ftp.list(ftpPath(folder))
    let days = Array.from(new Set(files.map(f => f.name.split('-').shift()).filter(f => f.length === 8 && !f.includes('.'))))
      .map(day2date)
    days.sort()
    await log.info(`interval de jours dans les fichiers : début = ${days[0]}, fin = ${days[days.length - 1]}`)

    const historyData = await readHistoryData(dir, folder)
    let previousState = {}
    let previousDay
    const lastProcessedDay = int2day(historyData.lastProcessedDay)
    if (lastProcessedDay) {
      const nbProcessedDays = days.findIndex(d => d === lastProcessedDay) + 1
      await log.info(`nombre de jours déjà traités : ${nbProcessedDays}`)
      days = days.slice(nbProcessedDays)
      await log.info(`téléchargement de l'état au dernier jour traité ${lastProcessedDay}`)
      previousState = await readDailyState(ftp, dir, folder, lastProcessedDay, log)
      previousDay = lastProcessedDay
    }

    if (processingConfig.maxDays !== -1 && days.length > processingConfig.maxDays) {
      days = days.slice(0, processingConfig.maxDays)
      await log.info(`nombre de jours restreint par la configuration : ${days.length}`)
    }

    for (const day of days) {
      const state = await readDailyState(ftp, dir, folder, day, log)
      const { stats, bulk } = await require('./lib/diff-bulk')(previousState, state, previousDay, day, historyData)
      await log.info(`enregistrement des modifications pour le jour ${day} : ouvertures=${stats.created}, fermetures=${stats.closed}, modifications=${stats.updated}, inchangés=${stats.unmodified}`)
      while (bulk.length) {
        await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines`, bulk.splice(0, 1000))
      }
      historyData.lastProcessedDay = day2int(day)
      await historyData.write()
      if (previousDay) await clearFiles(dir, folder, previousDay, log)
      previousState = state
      previousDay = day
    }
  }
}

const ftpPath = (folder) => `/www/sites/default/files/private/${folder}/archive`
