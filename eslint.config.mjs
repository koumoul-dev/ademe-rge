import neostandard from 'neostandard'

export default [
  { ignores: ['config/local-*', 'data/', 'misc/', 'node_modules/', 'qualifelec-ftp/', 'resources/history-proto.js'] },
  ...neostandard(),
  {
    // mocha globals
    files: ['test/**/*.js'],
    languageOptions: {
      globals: {
        describe: 'readonly',
        it: 'readonly',
        before: 'readonly',
        after: 'readonly',
        beforeEach: 'readonly',
        afterEach: 'readonly'
      }
    }
  }
]
