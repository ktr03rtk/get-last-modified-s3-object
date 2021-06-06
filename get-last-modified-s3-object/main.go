package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sort"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3ListObjectsAPI defines the interface for the ListObjectsV2 function.
// We use this interface to test the function using a mocked service.
type S3ListObjectsAPI interface {
	ListObjectsV2(ctx context.Context,
		params *s3.ListObjectsV2Input,
		optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// GetObjects retrieves the objects in an Amazon Simple Storage Service (Amazon S3) bucket
// Inputs:
//     c is the context of the method call, which includes the AWS Region
//     api is the interface that defines the method call
//     input defines the input arguments to the service call.
// Output:
//     If success, a ListObjectsV2Output object containing the result of the service call and nil
//     Otherwise, nil and an error from the call to ListObjectsV2
func GetObjects(c context.Context, api S3ListObjectsAPI, input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	return api.ListObjectsV2(c, input)
}

func handler(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	bucket := os.Getenv("BUCKET_NAME")
	prefix := os.Getenv("PREFIX")
	region := os.Getenv("REGION")

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		panic("configuration error, " + err.Error())
	}

	client := s3.NewFromConfig(cfg)

	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	resp, err := client.ListObjectsV2(context.TODO(), listInput)
	if err != nil {
		fmt.Println("Got error retrieving list of objects:")
		fmt.Println(err)
		log.Fatal(err)
	}

	sort.Slice(resp.Contents, func(i, j int) bool {
		return resp.Contents[i].LastModified.After(*resp.Contents[j].LastModified)
	})

	for _, item := range resp.Contents {
		fmt.Println("Name:          ", *item.Key)
		fmt.Println("Last modified: ", *item.LastModified)
		fmt.Println("")
	}

	var outputBuf bytes.Buffer
	outputBuf.WriteString(fmt.Sprintf("Last uploaded S3 object: %v/%v\n", bucket, *resp.Contents[0].Key))
	outputBuf.WriteString(fmt.Sprintf("Last uploaded time: %v\n", *resp.Contents[0].LastModified))

	objectInput := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(*resp.Contents[0].Key),
	}

	result, err := client.GetObject(context.TODO(), objectInput)
	if err != nil {
		fmt.Println("Got error retrieving object:")
		fmt.Println(err)
		log.Fatal(err)
	}
	defer result.Body.Close()

	gr, err := gzip.NewReader(result.Body)
	if err != nil {
		fmt.Println("Got error retrieving object:")
		fmt.Println(err)
		log.Fatal(err)
	}
	defer gr.Close()

	b := bufio.NewReader(gr)
	defer gr.Close()

	outputBuf.WriteString("Object contents: ")

	for range make([]int, 5) {
		line, _, err := b.ReadLine()
		fmt.Println(string(line))
		outputBuf.Write(line)
		outputBuf.Write([]byte{0x0A})
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Got error reading object:")
			fmt.Println(err)
			log.Fatal(err)
		}
	}

	fmt.Println("Found", len(resp.Contents), "items in bucket", bucket)
	fmt.Println("")

	return events.APIGatewayProxyResponse{
		Body:       fmt.Sprintf(outputBuf.String()),
		StatusCode: 200,
	}, nil
}

func main() {
	lambda.Start(handler)
}
