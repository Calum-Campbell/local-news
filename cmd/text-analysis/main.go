package main

import (
	"io/ioutil"
	"os"

	"encoding/json"

	"log"

	"textanalysis/internal"

	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/comprehend"
	"github.com/pkg/errors"
)

// Entity will be split up into people, places, times, phrases
type AnalysisResult struct {
	DataSource           string
	TopNegativeSentiment []internal.SentimentResult
	People               []internal.Entity
	Places               []internal.Entity
	Dates                []internal.Entity
	Organisations        []internal.Entity
	KeyPhrases           []internal.KeyPhrase
}

func main() {
	// pass the input s3 file name
	s3FileName := os.Args[1]
	outputFileName := os.Args[2]

	sess, err := CreateSession("identity")
	if err != nil {
		log.Fatal(err)
	}
	client := comprehend.New(sess)

	fmt.Print("Beginning text analysis")

	// Entities
	entityJobId := internal.StartEntitiesJob(client, s3FileName)
	entityOutputPath := internal.GetEntitiesFileOutputPath(client, entityJobId)
	entities := internal.EntityFileToJson(*entityOutputPath, sess)
	analyseEntities := internal.AnalyseEntities(entities)

	// Key phrases
	keyPhrasesJobId := internal.StartKeyPhrasesJob(client, s3FileName)
	keyPhrasesOutputPath := internal.GetKeyPhrasesFileOutputPath(client, keyPhrasesJobId)
	keyPhrases := internal.KeyPhrasesFileToJson(*keyPhrasesOutputPath, sess)
	analyseKeyPhrases := internal.AnalyseKeyPhrases(keyPhrases)

	// Sentiment
	fileContentAsString := internal.GetText(sess, s3FileName)
	analyseSentiment := internal.AnalyseTextSentiment(client, fileContentAsString)

	result := AnalysisResult{
		DataSource:           s3FileName,
		TopNegativeSentiment: analyseSentiment,
		People:               analyseEntities.People,
		Places:               analyseEntities.Places,
		Dates:                analyseEntities.Dates,
		Organisations:        analyseEntities.Organisations,
		KeyPhrases:           analyseKeyPhrases,
	}

	fmt.Printf("Writing text analysis to output file: %s", outputFileName)

	file, _ := json.MarshalIndent(result, "", " ")
	ioutil.WriteFile(outputFileName, file, 0644)

}

func CreateSession(profile string) (*session.Session, error) {
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
