package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	Produce()
}

func Produce() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": KafkaBootstrapServers,
		"acks":              "all",
	})

	if err != nil {
		if DebugMode {
			fmt.Printf("Failed to create producer: %s\n", err)
		}
		os.Exit(1)
	}

	defer p.Close()

	toProduce := map[string]string{
		KafkaFhvTopic:   "./data/fhv_tripdata_2019-01.csv",
		KafkaGreenTopic: "./data/green_tripdata_2019-01.csv",
	}

	for topic, datapath := range toProduce {
		produce(p, topic, datapath)
	}

	p.Flush(15 * 1000)
}

func produce(p *kafka.Producer, topic string, datapath string) {

	data, err := readCsv(datapath)

	if err != nil {
		fmt.Printf("Failed to read csv file: %s\n", err)
		return
	}

	data.Read()

	data_chan := make(chan kafka.Event, 10000)

	for i := 0; i < 5 || !DebugMode; i++ {
		record, err := data.Read()

		if err == io.EOF {
			fmt.Printf("Finished producing records to topic %s\n", topic)
			break
		}

		if err != nil && DebugMode {
			fmt.Printf("Failed to read record: %s\n", err)
			continue
		}

		if DebugMode {
			fmt.Printf("Producing record to topic %s: %s\n", topic, strings.Join(record, ", "))
		}

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(record[0]),
			Value:          []byte(strings.Join(record, ", ")),
		}, data_chan)

		if err != nil {
			if DebugMode {
				fmt.Printf("Failed to produce record: %s\n", err)
			}

			continue
		}

	}
}

func readCsv(filename string) (*csv.Reader, error) {
	csvFile, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Failed to open csv file: %s\n", err)
		return nil, err
	}

	reader := csv.NewReader(csvFile)
	reader.FieldsPerRecord = -1

	return reader, nil
}
