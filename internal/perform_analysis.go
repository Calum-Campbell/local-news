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
	s3FileName string,
	sess *session.Session) ([]SentimentResult, error) {
	merr := NewMultiError()

	fileContentAsString, err := GetText(sess, s3FileName)
	if err != nil {
		merr.AddError(errors.Wrap(err, "Unable to get file from S3"))
	}

	analyseSentiment, err := AnalyseTextSentiment(client, fileContentAsString)
	if err != nil {
		merr.AddError(errors.Wrap(err, "Unable to perform sentiment analysis job"))
	}

	return analyseSentiment, merr.Build()
}

func PerformEntityAnalysis(
	client *comprehend.Comprehend,
	s3FileName string,
	sess *session.Session) (TypedEntityResult, error) {
	merr := NewMultiError()

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

	analyseEntities := AnalyseEntities(entities)

	return analyseEntities, merr.Build()
}

func PerformKeyPhraseAnalysis(
	client *comprehend.Comprehend,
	s3FileName string,
	sess *session.Session) ([]KeyPhrase, error) {
	merr := NewMultiError()

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

	analyseKeyPhrases := AnalyseKeyPhrases(keyPhrases)

	return analyseKeyPhrases, nil
}

func PerformAnalysis(
	client *comprehend.Comprehend,
	s3FileName string,
	sess *session.Session) (AnalysisResult, error) {

	log.Println("Beginning text analysis")

	var result AnalysisResult
	merr := NewMultiError()
	var mux sync.Mutex
	var wg sync.WaitGroup

	wg.Add(3)

	go func() {
		defer wg.Done()
		sentimentResult, err := PerformSentimentAnalysis(client, s3FileName, sess)
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
		entityResult, err := PerformEntityAnalysis(client, s3FileName, sess)
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
		keyPhraseResult, err := PerformKeyPhraseAnalysis(client, s3FileName, sess)
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
