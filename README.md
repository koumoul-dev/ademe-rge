# ADEME RGE processing script

Generates an historical file from data from qualification organisms

## Running tests

Install dependencies:

```
npm install
```

Create a `config/local-test.js` file with the same structure as `config/default.js` but with filled values.
Then run the test suite:

```
npm test
```

## Release

The plugin is published to the data-fair registry by GitHub Actions:

- every push on `master` publishes to the **staging** registry (`publish-staging.yml`)
- pushing a `v*` tag publishes to the **production** registry (`publish-production.yml`)

The tag must match the version in `package.json`, otherwise the workflow fails.

```
npm version minor
git push && git push --tags
```

Both workflows need a `REGISTRY_API_KEY` secret, defined on the `staging` and `production`
GitHub environments of the repository.
