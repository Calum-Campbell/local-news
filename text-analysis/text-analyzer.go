package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"sort"

	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/comprehend"
	"github.com/pkg/errors"
)

type AnalysisResult struct {
	DataSource           string
	TopNegativeSentiment []SentimentResult
}

func main() {

	inputFileName := os.Args[1]
	outputFileName := os.Args[2]

	fileContent, err := ioutil.ReadFile(inputFileName) // just pass the file name
	if err != nil {
		fmt.Print(err)
	}

	fileContentAsString := string(fileContent)

	client, err := CreateComprehendClient("identity")

	analysisSentiment, err := AnalyseTextSentiment(client, fileContentAsString)
	if err != nil {
		fmt.Print(err)
	}

	result := AnalysisResult{
		DataSource:           inputFileName,
		TopNegativeSentiment: analysisSentiment,
	}

	file, _ := json.MarshalIndent(result, "", " ")
	ioutil.WriteFile(outputFileName, file, 0644)

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

type SentimentResult struct {
	Sentence          string
	NegativeSentiment *float64
}

func AnalyseTextSentiment(client *comprehend.Comprehend, text string) ([]SentimentResult, error) {
	sentences := strings.Split(text, ". ")
	var sentimentArray []SentimentResult

	for _, sentence := range sentences {
		sentenceSentimentAnalysis, err := AnalyseSentenceSentiment(client, sentence)
		if err != nil {
			return sentimentArray, err
		}
		sentimentArray = append(sentimentArray, sentenceSentimentAnalysis)
	}
	sort.Slice(sentimentArray, func(i, j int) bool {
		return *sentimentArray[i].NegativeSentiment > *sentimentArray[j].NegativeSentiment
	})
	return sentimentArray[0:6], nil
}

func AnalyseSentenceSentiment(client *comprehend.Comprehend, sentence string) (SentimentResult, error) {
	input := &comprehend.BatchDetectSentimentInput{}
	input.SetLanguageCode("en")
	input.SetTextList([]*string{aws.String(sentence)})
	result, err := client.BatchDetectSentiment(input)
	return SentimentResult{sentence, result.ResultList[0].SentimentScore.Negative}, err
}
