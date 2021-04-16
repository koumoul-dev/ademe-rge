process.env.NODE_ENV = 'test'
const config = require('config')
const axios = require('axios')
const chalk = require('chalk')
const moment = require('moment')
const fs = require('fs-extra')
const assert = require('assert').strict
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

  it('should run a task', async function () {
    this.timeout(120000)

    const axiosInstance = axios.create({
      baseURL: config.dataFairUrl,
      headers: { 'x-apiKey': config.dataFairAPIKey },
      maxContentLength: Infinity,
      maxBodyLength: Infinity
    })
    // customize axios errors for shorter stack traces when a request fails
    axiosInstance.interceptors.response.use(response => response, error => {
      if (!error.response) return Promise.reject(error)
      delete error.response.request
      error.response.config = { method: error.response.config.method, url: error.response.config.url, data: error.response.config.data }
      return Promise.reject(error.response)
    })
    await fs.emptyDir('data/tmp')
    await ademeRGE.run({
      pluginConfig: {
        ftpOptions: config.ftpOptions
      },
      processingConfig: {
        dataset: { id: 'historique-rge-test', title: 'Historique RGE test', overwrite: false },
        folders: ['qualifelec'],
        maxDays: 10
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
      tmpDir: 'data/tmp'
    })

    /* const dataset = (await axiosInstance.get('api/v1/datasets/hello-world-test')).data
    assert.equal(dataset.title, 'Hello world test')
    const lines = (await axiosInstance.get('api/v1/datasets/hello-world-test/lines')).data.results
    assert.equal(lines.length, 1)
    assert.equal(lines[0]._id, 'hello')
    assert.equal(lines[0].message, 'Hello world test !') */
  })
})
