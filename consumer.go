package punter

import (
	"fmt"

	"github.com/juju/loggo"
	"github.com/streadway/amqp"
)

var (
	log = loggo.GetLogger("punter")
)

// MsgHander function which is supplied to handle delivers.
type MsgHander func(deliveries <-chan amqp.Delivery, done chan error)

// Consumer holds state for the AMQP consumer
type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
	count   int
}

// Config is the settings for the consumer
type Config struct {
	AmqpURI      string
	Exchange     string
	ExchangeType string
	QueueName    string
	Key          string
	MessageTTL   int32 // How long to retain messages in the queue
	Durable      bool  // Queue durable?
	Prefetch     int
}

// NewConsumer create and configure a new consumer, this also triggers a connection to AMQP server
func NewConsumer(config *Config, ctag string, msgHandler MsgHander) (*Consumer, error) {

	c := &Consumer{
		conn:    nil,
		channel: nil,
		tag:     ctag,
		done:    make(chan error),
	}

	var err error

	log.Infof("dialing %q", config.AmqpURI)
	c.conn, err = amqp.Dial(config.AmqpURI)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	go func() {
		log.Infof("closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	log.Infof("got Connection, getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	log.Infof("got Channel, declaring Exchange (%q)", config.Exchange)
	if err = c.channel.ExchangeDeclare(
		config.Exchange,     // name of the exchange
		config.ExchangeType, // type
		true,                // durable
		false,               // delete when complete
		false,               // internal
		false,               // noWait
		nil,                 // arguments
	); err != nil {
		return nil, fmt.Errorf("Exchange Declare: %s", err)
	}
	qargs := amqp.Table{}

	// if the value is set then configure the argument
	if config.MessageTTL > 0 {
		qargs["x-message-ttl"] = config.MessageTTL
	}

	log.Infof("declared Exchange, declaring Queue %q", config.QueueName)
	queue, err := c.channel.QueueDeclare(
		config.QueueName, // name of the queue
		config.Durable,   // durable
		false,            // delete when usused
		false,            // exclusive
		false,            // noWait
		qargs,            // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
	}

	// Set the QoS if necessary
	if config.Prefetch > 0 {
		if err := c.channel.Qos(config.Prefetch, 0, false); err != nil {
			return nil, fmt.Errorf("Failed to set Qos prefetch! Got: %s", err)
		}
	}

	log.Infof("declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
		queue.Name, queue.Messages, queue.Consumers, config.Key)

	args := amqp.Table{}
	// TODO make this configurable.
	args["x-expires"] = int32(30000) // 30 second

	if err = c.channel.QueueBind(
		queue.Name,      // name of the queue
		config.Key,      // bindingKey
		config.Exchange, // sourceExchange
		false,           // noWait
		args,            // arguments
	); err != nil {
		return nil, fmt.Errorf("Queue Bind: %s", err)
	}

	log.Infof("Queue bound to Exchange, starting Consume (consumer tag %q)", c.tag)
	deliveries, err := c.channel.Consume(
		queue.Name, // name
		c.tag,      // consumerTag,
		false,      // noAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Consume: %s", err)
	}

	go msgHandler(deliveries, c.done)

	return c, nil
}

// Shutdown enables cleanup of AMQP connection
func (c *Consumer) Shutdown() error {
	// will close() the deliveries channel
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Infof("AMQP shutdown OK")

	// wait for handle() to exit
	return <-c.done
}
