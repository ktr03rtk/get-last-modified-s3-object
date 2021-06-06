package main

import (
	"context"
	"fmt"
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
	bucket := aws.String(os.Getenv("BUCKET_NAME"))
	prefix := aws.String(os.Getenv("PREFIX"))
	region := os.Getenv("REGION")

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		panic("configuration error, " + err.Error())
	}

	client := s3.NewFromConfig(cfg)

	input := &s3.ListObjectsV2Input{
		Bucket: bucket,
		Prefix: prefix,
	}

	resp, err := GetObjects(context.TODO(), client, input)
	if err != nil {
		fmt.Println("Got error retrieving list of objects:")
		fmt.Println(err)
		log.Fatal(err)
	}

	sort.Slice(resp.Contents, func(i, j int) bool {
		return resp.Contents[i].LastModified.After(*resp.Contents[j].LastModified)
	})

	for _, item := range resp.Contents {
		// fmt.Println("Name:          ", *item.Key)
		fmt.Println("Last modified: ", *item.LastModified)
		// fmt.Println("Size:          ", item.Size)
		// fmt.Println("Storage class: ", item.StorageClass)
		// fmt.Println("")
	}

	fmt.Println("Found", len(resp.Contents), "items in bucket", bucket)
	fmt.Println("")

	return events.APIGatewayProxyResponse{
		Body:       fmt.Sprintf("Hello"),
		StatusCode: 200,
	}, nil
}

func main() {
	lambda.Start(handler)
}
