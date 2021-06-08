package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var (
	bucket  string
	envName string
	region  string
	lf      = byte(0x0A)
)

type MyEvent struct {
	UnitID string `json:"unit_id"`
	Sensor string `json:"sensor"`
}

type MyData struct {
	Timestamp string `json:"timestamp"`
	Data      string `json:"data"`
}

type Response struct {
	Title        string    `json:"title"`
	Bucket       string    `json:"bucket"`
	Key          string    `json:"key"`
	LastModified time.Time `json:"last_modified"`
	Contents     []MyData  `json:"contents"`
}

func init() {
	b, ok := os.LookupEnv("BUCKET_NAME")
	if !ok {
		log.Fatal("Got error LookupEnv: BUCKET_NAME")
	}
	bucket = b

	e, ok := os.LookupEnv("ENVIRONMENT_NAME")
	if !ok {
		log.Fatal("Got error LookupEnv: ENVIRONMENT_NAME")
	}
	envName = e

	r, ok := os.LookupEnv("REGION")
	if !ok {
		log.Fatal("Got error LookupEnv: REGION")
	}
	region = r
}

func handler(event MyEvent) (events.APIGatewayProxyResponse, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		log.Fatal(err.Error())
	}

	client := s3.NewFromConfig(cfg)

	prefix := fmt.Sprintf("%s/unit=%s/sensor=%s/%s", envName, event.UnitID, event.Sensor, time.Now().Format("year=2006/month=01/day=02"))

	obj := getLastModifiedObjectInfo(client, prefix)
	if obj.Key == nil {
		return events.APIGatewayProxyResponse{
			Body:       fmt.Sprintf("no object found. bucket: %s, prefix: %s", bucket, prefix),
			StatusCode: 200,
		}, nil
	}

	contents := getObjectBody(obj, client, *obj.Key)

	res := &Response{
		Title:        "Last uploaded S3 object",
		Bucket:       bucket,
		Key:          *obj.Key,
		LastModified: *obj.LastModified,
		Contents:     contents,
	}

	responseJson, err := json.Marshal(res)
	if err != nil {
		log.Fatal("Got error json marshal:", err)
	}

	var outputBuf bytes.Buffer
	if _, err := outputBuf.Write(responseJson); err != nil {
		log.Fatal("Got error WriteString:", err)
	}

	return events.APIGatewayProxyResponse{
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body:       fmt.Sprintf(outputBuf.String()),
		StatusCode: 200,
	}, nil
}

func getLastModifiedObjectInfo(client *s3.Client, prefix string) types.Object {
	var (
		lastModifiedObj types.Object
		token           *string
	)

	for {
		listInput := &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			MaxKeys:           1000,
			ContinuationToken: token,
		}

		res, err := client.ListObjectsV2(context.TODO(), listInput)
		if err != nil {
			log.Fatal("Got error retrieving list of objects:", err)
		}

		if res.Contents == nil {
			return *new(types.Object)
		}

		sort.Slice(res.Contents, func(i, j int) bool {
			return res.Contents[i].LastModified.After(*res.Contents[j].LastModified)
		})

		if lastModifiedObj.LastModified == nil || res.Contents[0].LastModified.After(*lastModifiedObj.LastModified) {
			lastModifiedObj = res.Contents[0]
		}

		if !res.IsTruncated {
			break
		}

		token = res.NextContinuationToken
	}

	return lastModifiedObj
}

func getObjectBody(obj types.Object, client *s3.Client, key string) []MyData {
	objectInput := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
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

	var (
		contents []MyData
		data     MyData
	)

	for range make([]int, 5) {
		line, _, err := b.ReadLine()
		if err := json.Unmarshal(line, &data); err != nil {
			log.Fatal("Got error unmarshal data:", err)
		}
		contents = append(contents, data)

		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal("Got error reading object:", err)
		}
	}

	return contents
}

func main() {
	lambda.Start(handler)
}
