const Logger = require('logger-sharelatex')
const { SettingsError } = require('./Errors')

function getPersistor(backend, settings) {
  switch (backend) {
    case 'aws-sdk':
    case 's3':
      const S3Persistor = require('./S3Persistor')
      return new S3Persistor(
        Object.assign({}, settings.s3, { Metrics: settings.Metrics })
      )
    case 'fs':
      const FSPersistor = require('./FSPersistor')
      return new FSPersistor({
        paths: settings.paths,
        Metrics: settings.Metrics
      })
    case 'gcs':
      const GcsPersistor = require('./GcsPersistor')
      return new GcsPersistor(
        Object.assign({}, settings.gcs, { Metrics: settings.Metrics })
      )
    default:
      throw new SettingsError('unknown backend', { backend })
  }
}

module.exports = function create(settings) {
  Logger.info(
    {
      backend: settings.backend,
      fallback: settings.fallback && settings.fallback.backend
    },
    'Loading backend'
  )
  if (!settings.backend) {
    throw new SettingsError('no backend specified - config incomplete')
  }

  let persistor = getPersistor(settings.backend, settings)

  if (settings.fallback && settings.fallback.backend) {
    const primary = persistor
    const fallback = getPersistor(settings.fallback.backend, settings)
    const MigrationPersistor = require('./MigrationPersistor')
    persistor = new MigrationPersistor(
      primary,
      fallback,
      Object.assign({}, settings.fallback, { Metrics: settings.Metrics })
    )
  }

  return persistor
}
