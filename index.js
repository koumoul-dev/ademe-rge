// main execution method
exports.run = async ({ pluginConfig, processingConfig, processingId, dir, axios, log, patchConfig, sendMail }) => {
  const fs = require('fs-extra')
  const path = require('path')

  await log.step('Connexion au serveur FTP')
  const FTPClient = require('promise-ftp')
  const ftp = new FTPClient()
  const serverMessage = await ftp.connect({
    host: 'localhost',
    port: 21,
    user: undefined,
    password: undefined,
    connTimeout: 30000,
    pasvTimeout: 30000,
    autoReconnect: true,
    ...pluginConfig.ftpOptions
  })
  pluginConfig.ftpBasePath = pluginConfig.ftpBasePath || '/www/sites/default/files/private/'
  await log.info('connecté : ' + serverMessage)

  await log.step('Connexion au serveur FTP historique')
  const oldFTP = new FTPClient()
  const oldServerMessage = await oldFTP.connect({
    host: 'localhost',
    port: 21,
    user: undefined,
    password: undefined,
    connTimeout: 30000,
    pasvTimeout: 30000,
    autoReconnect: true,
    ...pluginConfig.oldFTPOptions
  })
  await log.info('connecté : ' + oldServerMessage)

  await log.step('Récupération des données de référence liées')
  if (!processingConfig.datasetLienDomaineQualif) throw new Error('La configuration ne contient pas de lien vers un jeu de données "Historique RGE"')
  const qualifDomaineLines = (await axios.get(`api/v1/datasets/${processingConfig.datasetLienDomaineQualif.id}/lines`, { params: { size: 10000 } })).data.results
  const qualifDomaine = qualifDomaineLines.reduce((a, qd) => { a[qd.CODE_QUALIFICATION] = qd; return a }, {})
  await log.info(`${qualifDomaineLines.length} lignes dans les données de référence "${processingConfig.datasetLienDomaineQualif.title}"`)
  if (!processingConfig.datasetContactsOrganismes) throw new Error('La configuration ne contient pas de lien vers un jeu de données "Contacts organismes"')
  const contactsOrganismesLines = (await axios.get(`api/v1/datasets/${processingConfig.datasetContactsOrganismes.id}/lines`, { params: { size: 10000 } })).data.results
  const contactsOrganismes = contactsOrganismesLines.reduce((a, ca) => { a[ca.ORGANISME] = ca; return a }, {})
  await log.info(`${contactsOrganismesLines.length} lignes dans les données de référence "${processingConfig.datasetContactsOrganismes.title}"`)
  if (processingConfig.datasetLandingZone) {
    await log.info(`synchronisation des fichers déposés sur le jeu de données "${processingConfig.datasetLandingZone.title}"`)
    await axios.post(`api/v1/datasets/${processingConfig.datasetLandingZone.id}/_sync_attachments_lines`)
  }

  // read .tar.gz uploaded by partners, and move content to archive if it is valid  or error folder otherwise
  const { downloadAndValidate, moveToFtp, sendValidationErrors } = require('./lib/import-validate')
  const { syncOldFolder } = require('./lib/old-ftp')
  for (const folder of processingConfig.folders) {
    await log.step(`Synchronisation avec le ftp historique pour le répertoire ${folder}`)
    await syncOldFolder(oldFTP, ftp, pluginConfig.ftpBasePath, folder, log)

    await log.step(`Import et validation du répertoire ${folder}`)
    await log.info('récupération de la liste des fichiers dans le répertoire')

    const files = await ftp.list(path.join(pluginConfig.ftpBasePath, folder))
    const csvs = files.map(f => f.name).filter(f => ['entreprises.csv', 'qualifications.csv', 'liens.csv'].includes(f))
    if (csvs.length) {
      const errors = await downloadAndValidate(ftp, dir, folder, csvs, pluginConfig.ftpBasePath, log)
      if (errors.length) {
        await sendValidationErrors(folder, contactsOrganismes, errors, log, sendMail)
      }
      await moveToFtp(ftp, dir, folder, !!errors.length, pluginConfig.ftpBasePath, log)
    } else {
      await log.info('aucun fichier à importer')
    }
  }

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
  } else if (processingConfig.datasetMode === 'clean_update') {
    await log.step('Vérification du jeu de données')
    dataset = (await axios.get(`api/v1/datasets/${processingConfig.dataset.id}`)).data
    if (!dataset) throw new Error(`le jeu de données n'existe pas, id${processingConfig.dataset.id}`)
    await log.info(`le jeu de donnée existe, id="${dataset.id}", title="${dataset.title}"`)
    await axios.delete(`api/v1/datasets/${processingConfig.dataset.id}/lines`)
    await log.info('le jeu de donnée a été vidé de ses données')
    await fs.remove(dir)
    await log.info('le répertoire de travail a été vidé de ses données')
  }

  // read files in folder/archive and calculate history
  const { day2int, int2day, day2date } = require('./lib/format')
  const { readHistoryData } = require('./lib/history')
  const { readDailyState, clearFiles } = require('./lib/daily-state')
  const repairDomains = require('./lib/repair-domains')
  for (const folder of processingConfig.folders) {
    log.step(`Traitement du répertoire ${folder}`)
    await fs.ensureDir(path.join(dir, folder))
    await log.info(`récupération de la liste des fichiers dans le répertoire ${folder}/archive`)
    const files = await ftp.list(path.join(pluginConfig.ftpBasePath, folder, '/archive'))
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
      previousState = await readDailyState(ftp, dir, folder, lastProcessedDay, qualifDomaine, pluginConfig.ftpBasePath, log)
      previousDay = lastProcessedDay
    }

    if (processingConfig.maxDays !== -1 && days.length > processingConfig.maxDays) {
      days = days.slice(0, processingConfig.maxDays)
      await log.info(`nombre de jours restreint par la configuration : ${days.length}`)
    }

    for (const day of days) {
      const state = await readDailyState(ftp, dir, folder, day, qualifDomaine, pluginConfig.ftpBasePath, log)
      const { stats, bulk } = await require('./lib/diff-bulk')(previousState, state, previousDay, day, historyData)
      await log.info(`enregistrement des modifications pour le jour ${day} : ouvertures=${stats.created}, fermetures=${stats.closed}, modifications=${stats.updated}, inchangés=${stats.unmodified}`)
      while (bulk.length) {
        const lines = bulk.splice(0, 1000)
        const res = await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines`, lines)
        if (res.data.nbErrors) log.error(`${res.data.nbErrors} échecs sur ${lines.length} lignes à insérer`, res.data.errors)
      }
      historyData.lastProcessedDay = day2int(day)
      await historyData.write()
      if (previousDay) await clearFiles(dir, folder, previousDay, log)
      previousState = state
      previousDay = day
    }
  }
  await repairDomains(axios, dataset, qualifDomaine, log)

  if (processingConfig.datasetLandingZone) {
    await log.info(`synchronisation des fichers déposés sur le jeu de données "${processingConfig.datasetLandingZone.title}"`)
    await axios.post(`api/v1/datasets/${processingConfig.datasetLandingZone.id}/_sync_attachments_lines`)
  }
}
