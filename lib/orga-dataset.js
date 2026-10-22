const { toCSV } = require('./format')

// the columns typed by the organisms, data-fair computes the other ones of the ADEME format (Sirene and qualification extensions)
const fields = ['siret', 'telephone', 'email', 'site_internet', 'code_qualification', 'lien_date_debut', 'lien_date_fin', 'url_qualification', 'nom_certificat', 'particulier']

const lineKey = (line) => `${line.siret}|${line.code_qualification}`
const cell = (value) => value === undefined || value === null ? '' : String(value)
const pick = (row) => Object.fromEntries(fields.map(f => [f, row[f]]))

// bulk operations turning the current lines of the dataset into the rows, lines are addressed by the primary key siret + code_qualification
exports.diffLines = (lines, rows) => {
  const linesByKey = new Map(lines.map(line => [lineKey(line), line]))
  // a key can hold one line only: the last row wins, otherwise duplicated rows would overwrite each other on every run
  const rowsByKey = new Map(rows.map(row => [lineKey(row), row]))
  const bulk = []
  const stats = { upserted: 0, deleted: 0, unmodified: 0, duplicates: rows.length - rowsByKey.size }
  for (const [key, row] of rowsByKey) {
    const line = linesByKey.get(key)
    if (line && fields.every(f => cell(line[f]) === cell(row[f]))) {
      stats.unmodified += 1
    } else {
      bulk.push({ _action: 'createOrUpdate', ...pick(row) })
      stats.upserted += 1
    }
  }
  for (const [key, line] of linesByKey) {
    if (!rowsByKey.has(key)) {
      bulk.push({ _action: 'delete', _id: line._id })
      stats.deleted += 1
    }
  }
  return { bulk, stats }
}

const sendLines = async (axios, url, lines, log) => {
  const result = (await axios.post(url, lines)).data
  await log.info(`lignes chargées: ${result.nbOk.toLocaleString()} ok, ${result.nbErrors.toLocaleString()} en erreur`)
  if (result.nbErrors) {
    await log.error(`${result.nbErrors} erreurs rencontrées`)
    for (const error of result.errors) {
      await log.error(JSON.stringify(error))
      await log.error(JSON.stringify(lines[error.line]))
    }
  }
}

// an editable dataset only receives the differences, a file dataset gets its whole file replaced
exports.updateOrgaDataset = async (axios, orgaDataset, rows, folder, log) => {
  const dataset = (await axios.get(`api/v1/datasets/${orgaDataset.id}`)).data
  if (!dataset.isRest) {
    if (!dataset.file) throw new Error(`le jeu de données ${dataset.title} (${dataset.id}) n'est ni éditable ni un jeu fichier`)
    const form = new FormData()
    form.append('file', new Blob([toCSV(fields, rows)], { type: 'text/csv' }), `${folder}.csv`)
    await axios.post(`api/v1/datasets/${orgaDataset.id}`, form, { maxBodyLength: Infinity })
    await log.info(`fichier chargé : ${rows.length.toLocaleString()} lignes`)
    return
  }
  if ([...(dataset.primaryKey ?? [])].sort().join(',') !== 'code_qualification,siret') {
    // without this primary key the lines cannot be addressed, a diff would duplicate them
    await log.warning('le jeu de données n\'a pas la clé primaire siret, code_qualification : toutes ses lignes sont remplacées')
    await sendLines(axios, `api/v1/datasets/${orgaDataset.id}/_bulk_lines?drop=true`, rows.map(row => ({ _action: 'create', ...pick(row) })), log)
    return
  }
  let res = await axios.get(`api/v1/datasets/${orgaDataset.id}/lines`, { params: { size: 10000, select: ['_id', ...fields].join(',') } })
  const lines = res.data.results
  while (res.data.next) {
    res = await axios.get(res.data.next)
    lines.push(...res.data.results)
  }
  const { bulk, stats } = exports.diffLines(lines, rows)
  if (stats.duplicates) await log.warning(`${stats.duplicates} lignes en double sur siret, code_qualification : seule la dernière est gardée`)
  await log.info(`différences avec le jeu de données : ${stats.upserted} lignes créées ou modifiées, ${stats.deleted} supprimées, ${stats.unmodified} inchangées`)
  for (let i = 0; i < bulk.length; i += 1000) {
    await sendLines(axios, `api/v1/datasets/${orgaDataset.id}/_bulk_lines`, bulk.slice(i, i + 1000), log)
  }
}
