package main

import (
	"github.com/aws/aws-sdk-go/service/comprehend"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/pkg/errors"
	"os"
	"fmt"
	"io/ioutil"
)

func main() {

	fileName := os.Args[1]

	fileContent, err := ioutil.ReadFile(fileName) // just pass the file name
	if err != nil {
		fmt.Print(err)
	}

	fileContentAsString := string(fileContent)

	client, err := CreateComprehendClient("identity")
	fmt.Print(AnalyseText(client, fileContentAsString))
}

func CreateComprehendClient(profile string) (*comprehend.Comprehend, error) {
	sess, err := session.NewSession(&aws.Config{
	Region: aws.String("eu-west-1"),
	})

	if err != nil {
	return nil, errors.Wrap(err, "unable to create new sessions")
	}

	sess.Config.Credentials = credentials.NewCredentials(
	&credentials.SharedCredentialsProvider{
	Profile: profile,
	},
	)

	if _, err := sess.Config.Credentials.Get(); err != nil {
	return nil, errors.Wrap(err, "unable to get credentials")
	}

	return comprehend.New(sess), nil
}

func AnalyseText(client *comprehend.Comprehend, fileContent string) (*comprehend.BatchDetectSentimentOutput, error) {
	input := &comprehend.BatchDetectSentimentInput{}
	input.SetLanguageCode("en")
	input.SetTextList([]*string{aws.String(fileContent)})
	return client.BatchDetectSentiment(input)
}
