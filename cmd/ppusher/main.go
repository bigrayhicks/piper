package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/stojg/piper"
	"io"
	"log"
	"net"
	"os"
	"time"
)

const maxRecordSize = 50 * 1024

var (
	minTickDuration = 10000 * time.Millisecond
	maxTickDuration = 10000 * time.Millisecond
	maxRetries      = 3
)

func main() {
	pipeline := make(chan []byte, 1000)
	tcpListener, err := net.Listen("tcp", "127.0.0.1:2003")
	if err != nil {
		log.Fatal(err)
		os.Exit(2)
	}

	log.Printf("Listening on TCP %s", tcpListener.Addr())
	go listen(pipeline, tcpListener)
	sender(pipeline, sendToKinesis)
}

func listen(pipeline chan []byte, listener net.Listener) {
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Listener failed to accept connection: %s\n", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Send lines down the channel.
		reader := bufio.NewReader(conn)
		for {
			line, err := reader.ReadBytes('\n')

			// Returns err != nil if and only if the returned data does not end in delim.
			if err != nil {
				if err != io.EOF {
					log.Printf("Listener failed reading from connection: %s\n", err)
				}
				break
			}
			log.Printf("Recieved:\n%s", line)
			pipeline <- line
		}

		conn.Close()
	}
}

func sender(pipeline chan []byte, saveFunc func(*bytes.Buffer)) {
	record := new(bytes.Buffer)
	tickStart := time.Now()
	for {
		select {
		case line := <-pipeline:
			// Check if we are overflowing the 50kB maximum Kinesis message size.
			if len(line)+record.Len() > maxRecordSize {
				// Respect the minTick - we don't want to send more often than once per minTick.
				sinceStart := time.Since(tickStart)
				if sinceStart < minTickDuration {
					time.Sleep(minTickDuration - sinceStart)
				}

				saveFunc(record)
				tickStart = time.Now()
			}

			_, err := record.Write(line)
			if err != nil {
				log.Fatalf("Sender failed to write into the record buffer: %s\n", err)
			}
		default:
			// Poll for some more data to come.
			time.Sleep(1 * time.Second)
		}

		// Wait for data for maxTick, then send what we have.
		if time.Since(tickStart) > maxTickDuration {
			if record.Len() > 0 {
				saveFunc(record)
			}
			tickStart = time.Now()
		}
	}
}

func sendToKinesis(record *bytes.Buffer) {
	client := piper.NewKinesisClient()

	// Partition by hashing the data. This will be a bit random, but will at least ensure all shards are used
	// (if we ever have more than one)
	partitionKey := fmt.Sprintf("%x", md5.Sum(record.Bytes()))

	// Try a few times on error. The initial reason for this is Go AWS SDK seems to have some weird timing issue,
	// where sometimes the request would just EOF if requests are made in regular intervals. For example doing
	// "put-record" from us-west-1 to ap-southeast-2 every 6-7 seconds will cause EOF error, without the record being sent.
	for i, backoff := 0, time.Second; i < maxRetries; i, backoff = i+1, backoff*2 {

		_, err := client.PutRecord(&kinesis.PutRecordInput{
			Data:         record.Bytes(),
			PartitionKey: aws.String(partitionKey),
			StreamName:   aws.String(piper.StreamName),
		})

		if err == nil {
			log.Printf("Sent to kinesis:\n%s", record.String())
			break
		}

		// Send has failed.
		if i < maxRetries-1 {
			log.Printf("Retrying in %d s, sender failed to put record on try %d: %s.\n", backoff/time.Second, i, err)
			time.Sleep(backoff)
		} else {
			log.Printf("Aborting, sender failed to put record on try %d: %s.\n", i, err)
		}
	}

	record.Reset()
}
