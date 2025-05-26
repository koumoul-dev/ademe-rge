exports.getHttpErrorMessage = (err) => {
  let httpMessage = err.status ?? err.statusCode
  if (httpMessage) {
    const statusText = err.statusText ?? err.statusMessage
    if (statusText) httpMessage += ' - ' + statusText
    if (err.data) {
      if (typeof err.data === 'string') httpMessage += ' - ' + err.data
      else httpMessage += ' - ' + JSON.stringify(err.data)
    } else if (err.message) {
      httpMessage += ' - ' + err.message
    }
    return httpMessage
  }
}

exports.prepareAxiosError = (error) => {
  const response = error.response ?? error.request?.res ?? error.res
  if (!response) return error
  delete response.request
  const headers = {}
  if (response.headers?.location) headers.location = response.headers.location
  response.headers = headers
  response.config = response.config ?? error.config
  if (response.config) {
    response.config = { method: response.config.method, url: response.config.url, params: response.config.params, data: response.config.data }
    if (response.config.data && response.config.data._writableState) delete response.config.data
  }
  if (response.data && response.data._readableState) delete response.data
  if (error.message) response.message = error.message
  if (error.stack) response.stack = error.stack
  return response
}
