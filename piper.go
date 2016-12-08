package piper

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/aws/session"
	"fmt"
)

const StreamName = "test"

func NewKinesisClient() *kinesis.Kinesis {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("ap-southeast-2"),
	})
	if err != nil {
		fmt.Println("failed to create session,", err)
		return nil
	}
	return kinesis.New(sess)
}

