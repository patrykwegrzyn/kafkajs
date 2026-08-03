jest.mock('./defaults.test', () => ({
  ...jest.requireActual('./defaults'),
  initialRetryTime: 1,
  maxRetryTime: 1,
  factor: 0,
  multiplier: 1,
}))

const createRetry = require('.')

const alwaysFails = attempts => async () => {
  attempts.count += 1
  const error = new Error('transient failure')
  error.retriable = true
  throw error
}

describe('retry defaults', () => {
  it('uses the production default of five retries when options are omitted', async () => {
    const attempts = { count: 0 }

    await expect(createRetry()(alwaysFails(attempts))).rejects.toMatchObject({
      name: 'KafkaJSNumberOfRetriesExceeded',
      retryCount: 5,
    })

    expect(attempts.count).toBe(6)
  })

  it('allows an explicit retry count to override the default', async () => {
    const attempts = { count: 0 }

    await expect(createRetry({ retries: 2 })(alwaysFails(attempts))).rejects.toMatchObject({
      name: 'KafkaJSNumberOfRetriesExceeded',
      retryCount: 2,
    })

    expect(attempts.count).toBe(3)
  })
})
