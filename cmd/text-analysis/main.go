package main

import (
	"io/ioutil"
	"os"

	"encoding/json"

	"log"

	"textanalysis/internal"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/comprehend"
	"github.com/pkg/errors"
)

func main() {
	// pass the input s3 file name
	s3FileName := os.Args[1]
	outputFileName := os.Args[2]

	sess, err := CreateSession("identity")
	if err != nil {
		log.Fatal(err)
	}

	client := comprehend.New(sess)

	textBytes, err := internal.GetTextBytes(sess, s3FileName)
	if err != nil {
		log.Fatal(err)
	}

	result, err := internal.PerformAnalysis(client, s3FileName, sess, textBytes)
	if err != nil {
		log.Fatal(err)
	}

	WriteToFile(result, outputFileName)
}

func CreateSession(profile string) (*session.Session, error) {
	log.Println("Creating session")

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

	return sess, err
}

func WriteToFile(result internal.AnalysisResult, outputFileName string) {
	log.Printf("Writing text analysis to output file: %s", outputFileName)

	file, _ := json.MarshalIndent(result, "", " ")
	ioutil.WriteFile(outputFileName, file, 0644)
}
