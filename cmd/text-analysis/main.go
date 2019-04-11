package main

import (
	"encoding/json"
	"io/ioutil"

	"log"

	"textanalysis/internal"

	"flag"

	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/comprehend"
	"github.com/pkg/errors"
)

var (
	inputFile  = flag.String("input", "", "name of input file - should be in bucket whatif-local-news-le")
	outputFile = flag.String("output", "", "name of output file")
)

func main() {

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Run text analysis in developer playground:\n")
		flag.PrintDefaults()
	}

	// pass the input s3 file name
	flag.Parse()

	if *inputFile == "" || *outputFile == "" {
		fmt.Println("invalid input, to see usage: ./text-analysis -h")
		os.Exit(1)
	}

	sess, err := CreateSession("developerPlayground")
	if err != nil {
		log.Fatal(err)
	}

	client := comprehend.New(sess)

	textBytes, err := internal.GetTextBytes(sess, *inputFile)
	if err != nil {
		log.Fatal(err)
	}

	result, err := internal.PerformAnalysis(client, *inputFile, sess, textBytes)
	if err != nil {
		log.Fatal(err)
	}

	WriteToFile(result, *outputFile)
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
