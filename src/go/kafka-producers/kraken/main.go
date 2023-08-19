package main

import (
	"encoding/json"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	kraken_ws "github.com/aopoltorzhicky/go_kraken/websocket"
	log "github.com/sirupsen/logrus"

	"github.com/IBM/sarama"
)

var (
	brokers = getEnv("KAFKA_PROKERS", "redpanda-0:9092")
	topic = getEnv("KAFKA_TOPIC", "kraken-trades")

	tickers = []string{
		kraken_ws.ETHBTC,
		kraken_ws.USDTUSD,
		kraken_ws.ETHUSDT,
		kraken_ws.ETHEUR,
		kraken_ws.BTCUSD,
		kraken_ws.BTCEUR,
		kraken_ws.BTCJPY,
	}
)

type KafkaProducer struct {
	producer sarama.SyncProducer
}

type TradeWrapper struct {
	Ticker string
	kraken_ws.Trade
}

func main() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	kraken := newKrakenWebsocket()
	if err := kraken.Connect(); err != nil {
		log.Fatalf("Error connecting to web socket: %s", err.Error())
	}

	producer, err := newProducer()
	if err != nil {
		log.Fatalf("Error creating producer: %s", err.Error())
	}

	// subscribe to BTCUSD`s ticker
	if err := kraken.SubscribeTrades(tickers); err != nil {
		log.Fatalf("SubscribeTicker error: %s", err.Error())
	}

	tradeUpdates := make(chan TradeWrapper)

	go func() {
		for update := range tradeUpdates {
			log.Printf("----Ticker of %s----", update.Ticker)
			jsonUpdate, err := json.Marshal(update)
			if err != nil {
				log.Debugf("Failed to marshal update: %s", err.Error())
			}
			partition, offset, err := producer.Produce(string(jsonUpdate))
			if err != nil {
				log.Debugf("Failed to produce update: %s", err.Error())
			}
			log.Debugf("Produced message with offset %d to partition %d", offset, partition)
		}
	}()

	for {
		select {
		case <-signals:
			log.Warn("Stopping...")
			if err := kraken.Close(); err != nil {
				log.Fatal(err)
			}
			close(tradeUpdates)
			producer.producer.Close()
			return

		case update := <-kraken.Listen():
			switch data := update.Data.(type) {
			case []kraken_ws.Trade:
				for _, trade := range data {
					tradeUpdates <- TradeWrapper{
						Ticker: update.Pair,
						Trade: trade,
					}
				}
			default:
			}
		}
	}
}

func newKrakenWebsocket() *kraken_ws.Kraken {
	kraken := kraken_ws.NewKraken(
		kraken_ws.ProdBaseURL,
		kraken_ws.WithHeartbeatTimeout(10*time.Second),
		kraken_ws.WithLogLevel(log.TraceLevel),
		kraken_ws.WithReadTimeout(15*time.Second),
		kraken_ws.WithReconnectTimeout(5*time.Second),
	)
	return kraken
}

func newProducer() (*KafkaProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	brokerList := strings.Split(brokers, ",")
	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		return nil, err
	}

	return &KafkaProducer{producer: producer}, err
}

func (p *KafkaProducer) Produce(value string) (partition int32, offset int64, err error) {
	partition = int32(-1)
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(value),
	}
	partition, offset, err = p.producer.SendMessage(msg)

	return partition, offset, err
}


func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
