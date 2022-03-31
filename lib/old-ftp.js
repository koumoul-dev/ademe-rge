const path = require('path')

const oldBasePath = '/www/sites/default/files/private/'

// synchronize a folder from the old FTP to the new one
exports.syncOldFolder = async (oldFTP, ftp, ftpBasePath, folder, log) => {
  const baseFiles = await ftp.list(path.join(ftpBasePath, folder))
  for (const child of ['erreur', 'archive']) {
    if (!baseFiles.find(f => f.name === child)) {
      await log.info('création du répertoire manquant ' + path.join(ftpBasePath, folder, child))
      await ftp.mkdir(path.join(ftpBasePath, folder, child))
    }
    const oldFiles = (await oldFTP.list(path.join(oldBasePath, folder, child))).map(f => f.name)
    const newFiles = (await ftp.list(path.join(ftpBasePath, folder, child))).map(f => f.name)
    for (const f of oldFiles) {
      if (!newFiles.includes(f)) {
        await log.info('copie du fichier manquant ' + path.join(folder, child, f))
        const readStream = await oldFTP.get(path.join(oldBasePath, folder, child, f))
        await ftp.put(readStream, path.join(ftpBasePath, folder, child, f))
      }
    }
  }
}
