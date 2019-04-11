package internal

import (
	"log"
	"sync"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/comprehend"
	"github.com/pkg/errors"
)

type AnalysisResult struct {
	DataSource           string
	TopNegativeSentiment []SentimentResult
	People               []Entity
	Places               []Entity
	Dates                []Entity
	Organisations        []Entity
	KeyPhrases           []KeyPhrase
}

func PerformSentimentAnalysis(
	client *comprehend.Comprehend,
	text string) ([]SentimentResult, error) {
	var sentimentResult []SentimentResult
	analyseSentiment, err := AnalyseTextSentiment(client, text)
	if err != nil {
		return sentimentResult, err
	}

	sentimentResult = analyseSentiment
	return sentimentResult, nil
}

// If a file size is over 5000 bytes then we require it to use Amazon Comprehend's background 'asynchronous detection jobs'.
// This takes longer but can handle files up to 100,000 bytes
func PerformEntityAnalysis(
	client *comprehend.Comprehend,
	s3FileName string,
	sess *session.Session,
	textBytes []byte) (TypedEntityResult, error) {
	var entitiesArray []Entity
	merr := NewMultiError()

	if len(textBytes) >= 5000 {
		log.Println("File size is over 5000 bytes:", len(textBytes))
		entityJobId, err := StartEntitiesJob(client, s3FileName)
		if err != nil {
			merr.AddError(errors.Wrap(err, "Unable to perform entity analysis job"))
		}
		entityOutputPath, err := GetEntitiesFileOutputPath(client, entityJobId)
		if err != nil {
			merr.AddError(errors.Wrap(err, "Unable to get entity output path"))
		}

		entities, err := EntityFileToJson(*entityOutputPath, sess)
		if err != nil {
			merr.AddError(errors.Wrap(err, "Unable to download file and convert to JSON"))
		}
		entitiesArray = entities
	} else {
		log.Println("File size is under 5000 bytes:", len(textBytes))
		entities, err := SmallerFileEntityAnalysis(string(textBytes), client)
		if err != nil {
			merr.AddError(errors.Wrap(err, "Unable to perform entity analysis"))
		}
		entitiesArray = entities
	}

	analyseEntities := AnalyseEntities(entitiesArray)

	return analyseEntities, merr.Build()
}

// If a file size is over 5000 bytes then we require it to use Amazon Comprehend's background 'asynchronous detection jobs'.
// This takes longer but can handle files up to 100,000 bytes
func PerformKeyPhraseAnalysis(
	client *comprehend.Comprehend,
	s3FileName string,
	sess *session.Session,
	textBytes []byte) ([]KeyPhrase, error) {
	var keyPhrasesArray []KeyPhrase
	merr := NewMultiError()

	if len(textBytes) >= 5000 {
		log.Println("File size is over 5000 bytes:", len(textBytes))
		keyPhrasesJobId, err := StartKeyPhrasesJob(client, s3FileName)
		if err != nil {
			merr.AddError(errors.Wrap(err, "Unable to perform key phrase job"))
		}

		keyPhrasesOutputPath, err := GetKeyPhrasesFileOutputPath(client, keyPhrasesJobId)
		if err != nil {
			merr.AddError(errors.Wrap(err, "Unable to get key phrase output path"))
		}

		keyPhrases, err := KeyPhrasesFileToJson(*keyPhrasesOutputPath, sess)
		if err != nil {
			merr.AddError(errors.Wrap(err, "Unable to download file and convert to JSON"))
		}
		keyPhrasesArray = keyPhrases
	} else {
		log.Println("File size is under 5000 bytes:", len(textBytes))
		keyPhrases, err := SmallerFileKeyPhraseAnalysis(string(textBytes), client)
		if err != nil {
			merr.AddError(errors.Wrap(err, "Unable to perform key phrases analysis"))
		}
		keyPhrasesArray = keyPhrases
	}

	analyseKeyPhrases := AnalyseKeyPhrases(keyPhrasesArray)

	return analyseKeyPhrases, nil
}

func PerformAnalysis(
	client *comprehend.Comprehend,
	s3FileName string,
	sess *session.Session,
	textBytes []byte) (AnalysisResult, error) {

	log.Println("Beginning text analysis")

	var result AnalysisResult
	merr := NewMultiError()
	var mux sync.Mutex
	var wg sync.WaitGroup

	result.DataSource = s3FileName

	wg.Add(3)

	go func() {
		defer wg.Done()
		sentimentResult, err := PerformSentimentAnalysis(client, string(textBytes))
		if err != nil {
			merr.AddError(errors.Wrap(err, "Unable to perform sentiment analysis"))
			return
		}
		mux.Lock()
		result.TopNegativeSentiment = sentimentResult
		mux.Unlock()
	}()

	go func() {
		defer wg.Done()
		entityResult, err := PerformEntityAnalysis(client, s3FileName, sess, textBytes)
		if err != nil {
			merr.AddError(errors.Wrap(err, "Unable to perform entity analysis"))
			return
		}
		mux.Lock()
		result.People = entityResult.People
		result.Places = entityResult.Places
		result.Organisations = entityResult.Organisations
		result.Dates = entityResult.Dates
		mux.Unlock()
	}()

	go func() {
		defer wg.Done()
		keyPhraseResult, err := PerformKeyPhraseAnalysis(client, s3FileName, sess, textBytes)
		if err != nil {
			merr.AddError(errors.Wrap(err, "Unable to perform key phrase analysis"))
			return
		}
		mux.Lock()
		result.KeyPhrases = keyPhraseResult
		mux.Unlock()
	}()

	wg.Wait()

	return result, merr.Build()
}
