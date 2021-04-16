
exports.run = async ({ pluginConfig, processingConfig, processingId, tmpDir, axios, log }) => {
  const datasetSchema = require('./resources/schema.json')

  await log.step('Vérification du jeu de données')
  await log.info(`tentative de lecture du jeu ${processingConfig.dataset.id}`)
  try {
    const dataset = (await axios.get(`api/v1/datasets/${processingConfig.dataset.id}`)).data
    await log.debug('détail du jeu de données', dataset)
    if (dataset.extras && dataset.extras.processingId === processingId) {
      await log.info('le jeu de données existe et est rattaché à ce traitement')
    } else {
      if (processingConfig.dataset.overwrite) {
        await log.warning('le jeu de données existe et n\'est pas rattaché à ce traitement, l\'option "Surcharger un jeu existant" étant active le traitement peut continuer.')
        await axios.patch(`api/v1/datasets/${processingConfig.dataset.id}`, {
          extras: { ...dataset.extras, processingId }
        })
      } else {
        throw new Error('le jeu de données existe et n\'est pas rattaché à ce traitement')
      }
    }
    await axios.patch(`api/v1/datasets/${processingConfig.dataset.id}`, {
      title: processingConfig.dataset.title,
      schema: datasetSchema
    })
  } catch (err) {
    if (err.status !== 404) throw err
    await log.info('le jeu de données n\'existe pas encore')

    await log.step('Création du jeu de données')
    const createdDataset = (await axios.put(`api/v1/datasets/${processingConfig.dataset.id}`, {
      title: processingConfig.dataset.title,
      isRest: true,
      schema: datasetSchema,
      extras: { processingId }
    })).data
    await log.info('Le jeu a été créé')
    await log.debug('jeu de données créé', createdDataset)
  }

  const ftpOptions = {
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
  }
  await require('./lib/process-ftp')(tmpDir, ftpOptions, processingConfig.folders, processingConfig.maxDays, log)
}
