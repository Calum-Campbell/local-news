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

// Entity will be split up into people, places, times, phrases
type AnalysisResult struct {
	DataSource           string
	TopNegativeSentiment []SentimentResult
	People               []EntityTypeResult
	Places               []EntityTypeResult
	Dates                []EntityTypeResult
	Organisations        []EntityTypeResult
	KeyPhrases           []KeyPhrasesResult
}

func main() {
	// pass the input file name and output file name

	inputFileName := os.Args[1]
	outputFileName := os.Args[2]

	fileContent, err := ioutil.ReadFile(inputFileName)
	if err != nil {
		fmt.Print(err)
	}

	fileContentAsString := string(fileContent)

	client, err := CreateComprehendClient("identity")

	analyseSentiment, err := AnalyseTextSentiment(client, fileContentAsString)
	if err != nil {
		fmt.Print(err)
	}

	analyseEntities, err := AnalyseTextEntities(client, fileContentAsString)
	if err != nil {
		fmt.Print(err)
	}

	analyseKeyPhrases, err := AnalyseKeyPhrases(client, fileContentAsString)
	if err != nil {
		fmt.Print(err)
	}

	result := AnalysisResult{
		DataSource:           inputFileName,
		TopNegativeSentiment: analyseSentiment,
		People:               analyseEntities.People,
		Places:               analyseEntities.Places,
		Dates:                analyseEntities.Dates,
		Organisations:        analyseEntities.Organisations,
		KeyPhrases:           analyseKeyPhrases,
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

// if we need to calculate the frequency of an array of strings

//func dup_count(entityList []string) map[string]int {
//
//	duplicate_frequency := make(map[string]int)
//
//	for _, entityText := range entityList {
//		// check if the item/element exist in the duplicate_frequency map
//
//		_, exist := duplicate_frequency[entityText]
//
//		if exist {
//			duplicate_frequency[entityText] += 1 // increase counter by 1 if already in the map
//		} else {
//			duplicate_frequency[entityText] = 1 // else start counting from 1
//		}
//	}
//	return duplicate_frequency
//}

type EntityResult struct {
	People        []EntityTypeResult
	Places        []EntityTypeResult
	Dates         []EntityTypeResult
	Organisations []EntityTypeResult
}

type EntityTypeResult struct {
	Text string
	Type string
}

func AnalyseTextEntities(client *comprehend.Comprehend, text string) (EntityResult, error) {
	var entityArray []EntityTypeResult
	input := &comprehend.BatchDetectEntitiesInput{}
	input.SetLanguageCode("en")
	input.SetTextList([]*string{aws.String(text)})
	entities, err := client.BatchDetectEntities(input)
	if err != nil {
		return EntityResult{}, err
	}
	for _, entity := range entities.ResultList[0].Entities {
		entityArray = append(entityArray, EntityTypeResult{Text: *entity.Text, Type: *entity.Type})
	}
	return AnalyseEntity(entityArray), err
}

func AddEntityIfUnique(typeArray []EntityTypeResult, entity EntityTypeResult) []EntityTypeResult {
	for _, i := range typeArray {
		if i.Text == entity.Text {
			return typeArray
		}
	}
	return append(typeArray, entity)
}

func AnalyseEntity(entityArray []EntityTypeResult) EntityResult {
	var people []EntityTypeResult
	var places []EntityTypeResult
	var dates []EntityTypeResult
	var organisations []EntityTypeResult

	for _, entity := range entityArray {
		switch entityType := entity.Type; entityType {
		case "PERSON":
			people = AddEntityIfUnique(people, EntityTypeResult{Text: entity.Text, Type: entity.Type})
		case "LOCATION":
			places = AddEntityIfUnique(places, EntityTypeResult{Text: entity.Text, Type: entity.Type})
		case "DATE":
			dates = AddEntityIfUnique(dates, EntityTypeResult{Text: entity.Text, Type: entity.Type})
		case "ORGANIZATION":
			organisations = AddEntityIfUnique(organisations, EntityTypeResult{Text: entity.Text, Type: entity.Type})
		}
	}
	return EntityResult{People: people, Places: places, Dates: dates, Organisations: organisations}
}

func AddKeyPhraseIfUnique(keyPhrasesArray []KeyPhrasesResult, keyPhrase KeyPhrasesResult) []KeyPhrasesResult {
	for _, i := range keyPhrasesArray {
		if i.Text == keyPhrase.Text {
			return keyPhrasesArray
		}
	}
	return append(keyPhrasesArray, keyPhrase)
}

type KeyPhrasesResult struct {
	Text string
}

func AnalyseKeyPhrases(client *comprehend.Comprehend, text string) ([]KeyPhrasesResult, error) {
	var keyPhrasesArray []KeyPhrasesResult
	input := &comprehend.BatchDetectKeyPhrasesInput{}
	input.SetLanguageCode("en")
	input.SetTextList([]*string{aws.String(text)})
	keyPhrases, err := client.BatchDetectKeyPhrases(input)
	if err != nil {
		return keyPhrasesArray, err
	}
	for _, keyPhrase := range keyPhrases.ResultList[0].KeyPhrases {
		keyPhrasesArray = AddKeyPhraseIfUnique(keyPhrasesArray, KeyPhrasesResult{Text: *keyPhrase.Text})
	}
	return keyPhrasesArray, err
}
