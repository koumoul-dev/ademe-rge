// tell data-fair-processings to persist the data directory for this processing
exports.preserveDir = true

// main execution method
exports.run = async ({ pluginConfig, processingConfig, processingId, dir, axios, log }) => {
  const fs = require('fs-extra')
  const path = require('path')

  const datasetSchema = require('./resources/schema.json')
  let dataset

  await log.step('Vérification du jeu de données')
  await log.info(`tentative de lecture du jeu ${processingConfig.dataset.id}`)
  try {
    dataset = (await axios.get(`api/v1/datasets/${processingConfig.dataset.id}`)).data
    await log.debug('détail du jeu de données', dataset)
    if (dataset.extras && dataset.extras.processingId === processingId) {
      await log.info('le jeu de données existe et est rattaché à ce traitement')
    } else {
      if (processingConfig.dataset.overwrite) {
        await log.warning('le jeu de données existe et n\'est pas rattaché à ce traitement, l\'option "Surcharger un jeu existant" étant active le traitement peut continuer.')
        dataset = (await axios.patch(`api/v1/datasets/${processingConfig.dataset.id}`, {
          extras: { ...dataset.extras, processingId }
        })).data
      } else {
        throw new Error('le jeu de données existe et n\'est pas rattaché à ce traitement')
      }
    }
    dataset = (await axios.patch(`api/v1/datasets/${processingConfig.dataset.id}`, {
      title: processingConfig.dataset.title,
      schema: datasetSchema
    })).data
  } catch (err) {
    if (err.status !== 404) throw err
    await log.info('le jeu de données n\'existe pas encore')

    await log.step('Création du jeu de données')
    dataset = (await axios.put(`api/v1/datasets/${processingConfig.dataset.id}`, {
      title: processingConfig.dataset.title,
      isRest: true,
      schema: datasetSchema,
      extras: { processingId }
    })).data
    await log.info('Le jeu de données a été créé')
    await log.debug('jeu de données créé', dataset)
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
      await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines`, bulk)
      historyData.lastProcessedDay = day2int(day)
      await historyData.write()
      if (previousDay) await clearFiles(dir, folder, previousDay, log)
      previousState = state
      previousDay = day
    }
  }
}

const ftpPath = (folder) => `/www/sites/default/files/private/${folder}/archive`
