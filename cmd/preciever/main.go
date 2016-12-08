package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/stojg/piper"
	"log"
	"os"
	"os/signal"
	"time"
)

var (
	tickDuration    = 1000 * time.Millisecond
	getRecordsLimit = 1000
)

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	receiver()

	<-c
	log.Println("Received signal, shutting down")
}

func receiver() {
	client := piper.NewKinesisClient()
	stream, err := client.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String(piper.StreamName),
	})
	if err != nil {
		log.Printf("Receiver failed to describe the stream: %s\n", err)
		return
	}
	client = nil

	for _, shard := range stream.StreamDescription.Shards {
		log.Printf("pulling from %s\n", *shard.ShardId)
		go pullFromKinesis(shard.ShardId)
	}
}

func pullFromKinesis(shardId *string) {
	var si *string
	ticker := time.NewTicker(tickDuration)

	for range ticker.C {
		if si == nil {
			si = getShardIterator(shardId)
		}

		client := piper.NewKinesisClient()
		recordsOutput, err := client.GetRecords(&kinesis.GetRecordsInput{
			Limit:         aws.Int64(int64(getRecordsLimit)),
			ShardIterator: si,
		})
		if err != nil {
			log.Printf("ShardReceiver failed to get records: %s\n", err)
			continue
		}

		// Use the new iterator provided - they expire in 300s.
		si = recordsOutput.NextShardIterator

		if len(recordsOutput.Records) == 0 {
			continue
		}

		for _, record := range recordsOutput.Records {
			log.Printf("Recieved from Kinesis:\n%s", record.Data)
		}
	}
}

func getShardIterator(shardId *string) *string {
	client := piper.NewKinesisClient()
	shardIteratorOutput, err := client.GetShardIterator(&kinesis.GetShardIteratorInput{
		ShardId:           shardId,
		ShardIteratorType: aws.String("LATEST"),
		StreamName:        aws.String(piper.StreamName),
	})
	if err != nil {
		// Let's retry next time around.
		log.Printf("ShardReceiver failed to get the iterator: %s\n", err)

	}
	return shardIteratorOutput.ShardIterator
}
