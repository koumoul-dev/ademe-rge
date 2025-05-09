process.env.NODE_ENV = 'test'
const config = require('config')
const axios = require('axios')
const chalk = require('chalk')
const moment = require('moment')
// const fs = require('fs-extra')
const assert = require('assert').strict
// to work without remote datasets
// const ademeRGE = require('./download-validate')
const ademeRGE = require('../')

describe('Hello world processing', () => {
  it('should expose a plugin config schema for super admins', async () => {
    const schema = require('../plugin-config-schema.json')
    assert.ok(schema)
  })

  it('should expose a processing config schema for users', async () => {
    const schema = require('../processing-config-schema.json')
    assert.ok(schema)
  })

  it.only('should run a task', async function () {
    this.timeout(2400000)

    const headers = { 'x-apiKey': config.dataFairAPIKey }
    const axiosInstance = axios.create({
      // baseURL: config.dataFairUrl,
      // headers,
      maxContentLength: Infinity,
      maxBodyLength: Infinity
    })
    // apply default base url and send api key when relevant
    axiosInstance.interceptors.request.use(cfg => {
      if (!/^https?:\/\//i.test(cfg.url)) {
        if (cfg.url.startsWith('/')) cfg.url = config.dataFairUrl + cfg.url
        else cfg.url = config.dataFairUrl + '/' + cfg.url
      }
      if (cfg.url.startsWith(config.dataFairUrl)) Object.assign(cfg.headers, headers)
      return cfg
    }, error => Promise.reject(error))

    // customize axios errors for shorter stack traces when a request fails
    axiosInstance.interceptors.response.use(response => response, error => {
      if (!error.response) return Promise.reject(error)
      delete error.response.request
      error.response.config = { method: error.response.config.method, url: error.response.config.url, data: error.response.config.data }
      return Promise.reject(error.response)
    })
    // await fs.emptyDir('data/')
    await ademeRGE.run({
      pluginConfig: {
        ftpOptions: config.ftpOptions,
        ftpBasePath: config.ftpBasePath
      },
      processingConfig: {
        // datasetMode: 'create',
        // dataset: { title: 'Historique RGE test 3', overwrite: false },
        datasetMode: 'update',
        dataset: { id: '32yun3lr46-skfjyl3ep0l69', title: 'Historique RGE test !' },
        folders: ['cnoa'],
        maxDays: 2,
        datasetLienDomaineQualif: { id: 'yemnncmqiu43awy1lig9-qya', title: 'RGE - Lien domaine qualification' },
        datasetContactsOrganismes: { id: 'contacts-organismes', title: 'RGE - Contacts organismes' },
        datasetsOrganismes: [{
          // dataset: { id: 'hzqyaczl777z6wve8lbp6d91', title: 'RGE - Crequal' },
          dataset: { id: 'k14zg3p3m93odlg9fzxnaqyj', title: 'RGE - Cnoa' },
          organisme: 'cnoa'
        }]
      },
      processingId: 'test',
      axios: axiosInstance,
      log: {
        step: (msg) => console.log(chalk.blue.bold.underline(`[${moment().format('LTS')}] ${msg}`)),
        error: (msg, extra) => console.log(chalk.red.bold(`[${moment().format('LTS')}] ${msg}`), extra),
        warning: (msg, extra) => console.log(chalk.red(`[${moment().format('LTS')}] ${msg}`), extra),
        info: (msg, extra) => console.log(chalk.blue(`[${moment().format('LTS')}] ${msg}`), extra),
        debug: (msg, extra) => {
          // console.log(`[${moment().format('LTS')}] ${msg}`, extra)
        }
      },
      dir: 'data/',
      patchConfig: async (patch) => {
        console.log('received config patch', patch)
        // Object.assign(processingConfig, patch)
      },
      sendMail: async (data) => {
        console.log('send mail', data)
      }
    })

    /* const dataset = (await axiosInstance.get('api/v1/datasets/hello-world-test')).data
    assert.equal(dataset.title, 'Hello world test')
    const lines = (await axiosInstance.get('api/v1/datasets/hello-world-test/lines')).data.results
    assert.equal(lines.length, 1)
    assert.equal(lines[0]._id, 'hello')
    assert.equal(lines[0].message, 'Hello world test !') */
  })
})
