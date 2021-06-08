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
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var (
	bucket string
	prefix string
	region string
	lf     = byte(0x0A)
)

func init() {
	b, ok := os.LookupEnv("BUCKET_NAME")
	if !ok {
		log.Fatal("Got error LookupEnv: BUCKET_NAME")
	}
	bucket = b

	p, ok := os.LookupEnv("PREFIX")
	if !ok {
		log.Fatal("Got error LookupEnv: PREFIX")
	}
	prefix = p

	r, ok := os.LookupEnv("REGION")
	if !ok {
		log.Fatal("Got error LookupEnv: REGION")
	}
	region = r
}

func handler(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		log.Fatal(err.Error())
	}

	client := s3.NewFromConfig(cfg)

	obj := getLastModifiedObjectInfo(client)

	objInfo := fmt.Sprintf("Last uploaded S3 object: %v/%v\nLast uploaded time: %v\n", bucket, *obj.Key, *obj.LastModified)

	var outputBuf bytes.Buffer
	if _, err := outputBuf.WriteString(objInfo); err != nil {
		log.Fatal("Got error WriteString:", err)
	}

	getObjectBody(obj, client, &outputBuf)

	return events.APIGatewayProxyResponse{
		Body:       fmt.Sprintf(outputBuf.String()),
		StatusCode: 200,
	}, nil
}

func getLastModifiedObjectInfo(client *s3.Client) types.Object {
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	resp, err := client.ListObjectsV2(context.TODO(), listInput)
	if err != nil {
		log.Fatal("Got error retrieving list of objects:", err)
	}

	sort.Slice(resp.Contents, func(i, j int) bool {
		return resp.Contents[i].LastModified.After(*resp.Contents[j].LastModified)
	})

	for _, item := range resp.Contents {
		fmt.Println("Name:          ", *item.Key)
		fmt.Println("Last modified: ", *item.LastModified)
		fmt.Println("")
	}
	return resp.Contents[0]
}

func getObjectBody(obj types.Object, client *s3.Client, outputBuf *bytes.Buffer) {
	objectInput := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(*obj.Key),
	}

	result, err := client.GetObject(context.TODO(), objectInput)
	if err != nil {
		log.Fatal("Got error retrieving object:", err)
	}
	defer result.Body.Close()

	gr, err := gzip.NewReader(result.Body)
	if err != nil {
		log.Fatal("Got error retrieving object:", err)
	}
	defer gr.Close()

	b := bufio.NewReader(gr)
	defer gr.Close()

	if _, err := outputBuf.WriteString("Object contents: "); err != nil {
		log.Fatal("Got error WriteString:", err)
	}

	for range make([]int, 5) {
		line, _, err := b.ReadLine()
		fmt.Println(string(line))
		if _, err := outputBuf.Write(append(line, lf)); err != nil {
			log.Fatal("Got error WriteString:", err)
		}
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal("Got error reading object:", err)
		}
	}
}

func main() {
	lambda.Start(handler)
}
