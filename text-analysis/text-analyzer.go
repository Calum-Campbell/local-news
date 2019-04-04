package main

import (
	"io/ioutil"
	"os"
	"strings"

	"sort"

	"encoding/json"

	"log"

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
		log.Fatal(err)
	}

	fileContentAsString := string(fileContent)

	client, err := CreateComprehendClient("identity")

	analyseSentiment, err := AnalyseTextSentiment(client, fileContentAsString)
	if err != nil {
		log.Fatal(err)
	}

	analyseEntities, err := AnalyseTextEntities(client, fileContentAsString)
	if err != nil {
		log.Fatal(err)
	}

	analyseKeyPhrases, err := AnalyseKeyPhrases(client, fileContentAsString)
	if err != nil {
		log.Fatal(err)
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
	Sentence             string
	SurroundingSentences string
	NegativeSentiment    *float64
}

func AnalyseTextSentiment(client *comprehend.Comprehend, text string) ([]SentimentResult, error) {
	sentences := strings.Split(text, ". ")
	var surroundingSentences string
	var sentimentArray []SentimentResult

	for i := 0; i <= len(sentences)-1; i++ {
		if len(sentences) >= 3 {
			switch sentenceIndex := i; sentenceIndex {
			case 0:
				surroundingSentences = strings.Join([]string{sentences[i], sentences[i+1], sentences[i+2]}, ". ")
			case len(sentences) - 1:
				surroundingSentences = strings.Join([]string{sentences[i-2], sentences[i-1], sentences[i]}, ". ")
			default:
				surroundingSentences = strings.Join([]string{sentences[i-1], sentences[i], sentences[i+1]}, ". ")
			}
		}
		sentenceSentimentAnalysis, err := AnalyseSentenceSentiment(client, sentences[i], surroundingSentences)
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

func AnalyseSentenceSentiment(client *comprehend.Comprehend, sentence string, surroundingSentences string) (SentimentResult, error) {
	input := &comprehend.DetectSentimentInput{}
	input.SetLanguageCode("en")
	input.SetText(sentence)
	result, err := client.DetectSentiment(input)
	return SentimentResult{sentence, surroundingSentences, result.SentimentScore.Negative}, err
}

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
	input := &comprehend.DetectEntitiesInput{}
	input.SetLanguageCode("en")
	input.SetText(text)
	entities, err := client.DetectEntities(input)
	if err != nil {
		return EntityResult{}, err
	}
	for _, entity := range entities.Entities {
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
	input := &comprehend.DetectKeyPhrasesInput{}
	input.SetLanguageCode("en")
	input.SetText(text)
	keyPhrases, err := client.DetectKeyPhrases(input)
	if err != nil {
		return keyPhrasesArray, err
	}
	for _, keyPhrase := range keyPhrases.KeyPhrases {
		keyPhrasesArray = AddKeyPhraseIfUnique(keyPhrasesArray, KeyPhrasesResult{Text: *keyPhrase.Text})
	}
	return keyPhrasesArray, err
}
