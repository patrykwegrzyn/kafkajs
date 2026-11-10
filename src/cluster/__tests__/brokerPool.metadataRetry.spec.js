const BrokerPool = require('../brokerPool')

const logger = {
  namespace: jest.fn(() => logger),
  debug: jest.fn(),
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
}

describe('Cluster > BrokerPool > metadata retry', () => {
  it('selects a fresh broker when the metadata broker disconnects', async () => {
    const brokerPool = new BrokerPool({
      connectionPoolBuilder: {},
      logger,
      retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 },
    })
    brokerPool.seedBroker = {
      connectionPool: { host: 'seed', port: 9092 },
    }

    const staleBroker = {
      metadata: jest.fn(() => {
        throw new Error('Broker not connected')
      }),
    }
    const freshBroker = {
      metadata: jest.fn(() => ({ brokers: [], topicMetadata: [] })),
    }
    brokerPool.findConnectedBroker = jest
      .fn()
      .mockResolvedValueOnce(staleBroker)
      .mockResolvedValueOnce(freshBroker)

    await brokerPool.refreshMetadata(['topic'])

    expect(brokerPool.findConnectedBroker).toHaveBeenCalledTimes(2)
    expect(staleBroker.metadata).toHaveBeenCalledTimes(1)
    expect(freshBroker.metadata).toHaveBeenCalledTimes(1)
    expect(brokerPool.metadata).toEqual({ brokers: [], topicMetadata: [] })
  })
})
