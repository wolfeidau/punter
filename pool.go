package punter

import (
	"fmt"
)

// ConsumerPool container for consumers
type ConsumerPool struct {
	consumers []*Consumer
}

// NewConsumerPool create a pool of consumers for a given configuration.
func NewConsumerPool(numWorkers int, consumerNamePrefix string, punterConfig *Config, msgHandler MsgHander) (*ConsumerPool, error) {
	pool := &ConsumerPool{}
	pool.consumers = []*Consumer{}

	for i := 0; i < numWorkers; i++ {

		consumer, err := NewConsumer(punterConfig, fmt.Sprintf(consumerNamePrefix+"-consumer0%d", i), msgHandler)
		if err != nil {
			return nil, err
		}

		pool.consumers = append(pool.consumers, consumer)
	}

	return pool, nil
}

// Shutdown closes all the consumers in the pool.
func (pool *ConsumerPool) Shutdown() {
	for n, consumer := range pool.consumers {
		log.Infof("shutting down consumer %d", n)
		if err := consumer.Shutdown(); err != nil {
			log.Infof("error during shutdown: %s", err)
		}
	}
}
