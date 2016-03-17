package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/spf13/viper"
	"github.com/streadway/amqp"
)

var (
	ErrRetryTask = errors.New("Retry task")
)

const (
	timeformat = "2006-01-02T15:04:05"
)


func init() {
	viper.SetConfigName("go-starbuck")
	viper.SetConfigType("json")

	viper.AddConfigPath("/etc/goapi")
	viper.AddConfigPath(".")

	err := viper.ReadInConfig()

	log.Print(viper.GetString("BROKER_URI"))

	if err != nil {
		log.Fatal(err.Error())
	}
}

func main() {

	c, err := NewConsumer(viper.GetString("BROKER_URI"),
		viper.GetString("EXCHANGE"),
		viper.GetString("EXCHANGE_TYPE"),
		viper.GetString("QUEUE"),
		viper.GetString("BINDING_KEY"),
		viper.GetString("CONSUMER_TAG"),
	)
	if err != nil {
		log.Fatalf("%s", err)
	}

	lifetime := viper.GetInt("MAX_LIFETIME")

	if lifetime > 0 {
		log.Printf("running for %s", lifetime)
		time.Sleep(time.Duration(lifetime))
	} else {
		log.Printf("running forever")
		select {}
	}

	log.Printf("shutting down")

	if err := c.Shutdown(); err != nil {
		log.Fatalf("error during shutdown: %s", err)
	}
}

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
}

func NewConsumer(amqpURI, exchange, exchangeType, queueName, key, ctag string) (*Consumer, error) {
	c := &Consumer{
		conn:    nil,
		channel: nil,
		tag:     ctag,
		done:    make(chan error),
	}

	var err error

	log.Println("Connecting:", amqpURI)
	c.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	go func() {
		fmt.Println("Closing", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	log.Printf("got Connection, getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	log.Printf("got Channel, declaring Exchange (%q)", exchange)
	if err = c.channel.ExchangeDeclare(
		exchange,     // name of the exchange
		exchangeType, // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return nil, fmt.Errorf("Exchange Declare: %s", err)
	}

	log.Printf("declared Exchange, declaring Queue %q", queueName)
	queue, err := c.channel.QueueDeclare(
		queueName, // name of the queue
		true,      // durable
		false,     // delete when usused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
	}

	log.Printf("declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
		queue.Name, queue.Messages, queue.Consumers, key)

	if err = c.channel.QueueBind(
		queue.Name, // name of the queue
		key,        // bindingKey
		exchange,   // sourceExchange
		false,      // noWait
		nil,        // arguments
	); err != nil {
		return nil, fmt.Errorf("Queue Bind: %s", err)
	}

	log.Printf("Queue bound to Exchange, starting Consume (consumer tag %q)", c.tag)
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

	log.Printf("start handling deliveries")
	go handle(deliveries, c.done)

	return c, nil
}

func (c *Consumer) Shutdown() error {
	// will close() the deliveries channel
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Printf("AMQP shutdown OK")

	// wait for handle() to exit
	return <-c.done
}

func handle(deliveries <-chan amqp.Delivery, done chan error) {
	for d := range deliveries {
		var msg map[string]interface{}

		err := json.Unmarshal(d.Body, &msg)

		if err != nil {
			log.Print(fmt.Errorf("JSON decode error: %s", err))
			d.Ack(false)
			continue
		}

		log.Print("Message received: ", msg)

		if msg["task"] == "starbuck.http_request" {
			go runTask(d, msg)
		} else {
			log.Print("Invalid task received: ", msg["task"])
			d.Nack(false, false)
		}
	}
	log.Println("Deliveries channel closed")
	done <- nil
}

func runTask(d amqp.Delivery, msg map[string]interface{}) {
	log.Println("Running task:", msg["id"])
	log.Println(msg)

	retries := 0
	delay := viper.GetInt("DEFAULT_RETRY_DELAY")
	maxRetries := viper.GetInt("MAX_RETRIES")

	eta := msg["eta"]
	if eta != nil {
		log.Println("Task eta:", msg["id"], msg["eta"])
		t, _ := time.Parse(timeformat, eta.(string))

		delta := time.Since(t).Seconds()

		if delta < 0 {
			delta = -delta
			log.Println("Waiting for eta:", delta)

			time.Sleep(time.Duration(delta) * time.Second)
		}

	}


	for retries < maxRetries {
		if retries > 0 {
			log.Println("Retrying:", msg["id"])
		}

		err := requestTask(d, msg)

		if err == ErrRetryTask {
			time.Sleep(time.Duration(delay) * time.Second)
			retries += 1
			continue
		} else {
			d.Ack(false)
			log.Println("Task successful:", msg["id"])
			return
		}
	}

	log.Println("Max retries exceeded:", msg["id"])
	d.Nack(false, false)
}

func requestTask(d amqp.Delivery, msg map[string]interface{}) (err error) {

	kwargs := msg["kwargs"].(map[string]interface{})

	var body []byte

	if kwargs["data"] != nil {
		copy(body[:], kwargs["data"].(string))
	}

	req, err := http.NewRequest(kwargs["method"].(string),
		kwargs["url"].(string),
		bytes.NewBuffer(body))

	if kwargs["headers"] != nil {
		headers := kwargs["headers"].(map[string]interface{})

		for k, v := range headers {
			req.Header.Set(k, v.(string))
		}
	}

	client := &http.Client{}

	log.Println("Running HTTP request:", msg["id"], req)

	resp, err := client.Do(req)

	if err != nil {
		log.Println("HTTP Request Error:", msg["id"], err)
		return ErrRetryTask
	}

	defer resp.Body.Close()

	ioutil.ReadAll(resp.Body)

	if resp.StatusCode > 299 {
		log.Println("HTTP Request failed:", resp.Status)
	}

	return nil
}
