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

	"github.com/pkg/errors"

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
	prefix := fmt.Sprintf("%s/unit=%s/sensor=%s/%s", envName, event.UnitID, event.Sensor, time.Now().Format("year=2006/month=01/day=02"))

	result, err := getLastModifiedObjectBody(event, prefix)
	if err != nil {
		log.Fatalf("bucket: %s, prefix: %s, err: %s", bucket, prefix, err)
	}

	return events.APIGatewayProxyResponse{
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body:       *result,
		StatusCode: 200,
	}, nil
}

func getLastModifiedObjectBody(event MyEvent, prefix string) (*string, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		return nil, errors.Wrap(err, "failed to aws sdk configure")
	}

	client := s3.NewFromConfig(cfg)

	key, time, err := getLastModifiedObjectInfo(client, prefix)
	if err != nil {
		return nil, err
	}

	result, err := getObjectBody(client, key)
	if err != nil {
		return nil, err
	}

	body, err := constructResponseBody(key, time, result)
	if err != nil {
		return nil, err
	}

	return body, nil
}

func getLastModifiedObjectInfo(client *s3.Client, prefix string) (*string, *time.Time, error) {
	var lastModifiedObj types.Object

	listInput := &s3.ListObjectsV2Input{
		Bucket:            aws.String(bucket),
		Prefix:            aws.String(prefix),
		MaxKeys:           1000,
		ContinuationToken: nil,
	}

	for {
		res, err := client.ListObjectsV2(context.TODO(), listInput)
		if err != nil {
			return nil, nil, errors.Wrap(err, "Got error retrieving list of objects")
		} else if res.Contents == nil {
			return nil, nil, errors.New("Object not found")
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

		listInput.ContinuationToken = res.NextContinuationToken
	}

	return lastModifiedObj.Key, lastModifiedObj.LastModified, nil
}

func getObjectBody(client *s3.Client, key *string) ([]MyData, error) {
	objectInput := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    key,
	}

	obj, err := client.GetObject(context.TODO(), objectInput)
	if err != nil {
		return nil, errors.Wrap(err, "Got error retrieving object")
	}
	defer obj.Body.Close()

	gr, err := gzip.NewReader(obj.Body)
	if err != nil {
		return nil, errors.Wrap(err, "Got error constructing gzip Reader")
	}
	defer gr.Close()

	b := bufio.NewReader(gr)

	var (
		result []MyData
		data   MyData
	)

	for range make([]int, 5) {
		line, _, err := b.ReadLine()

		if err := json.Unmarshal(line, &data); err != nil {
			return nil, errors.Wrap(err, "Got error json unmarshal")
		}

		result = append(result, data)

		if err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.Wrap(err, "Got error reading object")
		}
	}

	return result, nil
}

func constructResponseBody(key *string, time *time.Time, result []MyData) (*string, error) {
	res := &Response{
		Title:        "Last uploaded S3 object",
		Bucket:       bucket,
		Key:          *key,
		LastModified: *time,
		Contents:     result,
	}

	responseJson, err := json.Marshal(res)
	if err != nil {
		return nil, errors.Wrap(err, "Got error json marshal")
	}

	var outputBuf bytes.Buffer
	if _, err := outputBuf.Write(responseJson); err != nil {
		return nil, errors.Wrap(err, "Got error WriteString")
	}

	body := outputBuf.String()

	return &body, nil
}

func main() {
	lambda.Start(handler)
}
