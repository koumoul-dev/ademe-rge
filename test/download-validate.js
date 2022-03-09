// main execution method
exports.run = async ({ pluginConfig, processingConfig, dir, log }) => {
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

  // read .tar.gz uploaded by partners, and move content to archive if it is valid  or error folder otherwise
  const { downloadAndValidate } = require('../lib/import-validate')
  for (const folder of processingConfig.folders) {
    log.step(`Import et validation du répertoire ${folder}`)
    await log.info('récupération de la liste des fichiers dans le répertoire')
    const files = await ftp.list(ftpPath(folder))
    const csvs = files.map(f => f.name).filter(f => f.endsWith('.csv'))
    if (csvs.length) {
      const errors = await downloadAndValidate(ftp, dir, folder, csvs, log)
      if (errors.length) {
        console.log('errors', errors)
      }
    } else {
      await log.info('aucun fichier à importer')
    }
  }
}

const ftpPath = (folder) => `/www/sites/default/files/private/${folder}`
