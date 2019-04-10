package internal

import (
	"log"

	"sort"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/comprehend"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type SentimentResult struct {
	Sentence             string
	SurroundingSentences string
	NegativeSentiment    *float64
}

func GetText(session *session.Session, fileName string) (string, error) {
	var text string
	item := fileName
	bucket := "lauren-temp"
	writer := aws.NewWriteAtBuffer([]byte{})
	downloader := s3manager.NewDownloader(session)

	log.Println("Downloading text file for sentiment analysis")
	_, err := downloader.Download(writer,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(item),
		})

	if err != nil {
		return text, err
	}
	text = string(writer.Bytes())
	return text, nil
}

func AnalyseTextSentiment(client *comprehend.Comprehend, text string) ([]SentimentResult, error) {
	sentences := strings.Split(text, ". ")
	var surroundingSentences string
	var sentimentArray []SentimentResult

	log.Println("Analysing sentences for sentiment")
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
	log.Println("Structuring sentiment data")
	sort.Slice(sentimentArray, func(i, j int) bool {
		return *sentimentArray[i].NegativeSentiment > *sentimentArray[j].NegativeSentiment
	})
	return sentimentArray[0:10], nil
}

func AnalyseSentenceSentiment(client *comprehend.Comprehend, sentence string, surroundingSentences string) (SentimentResult, error) {
	input := &comprehend.DetectSentimentInput{}
	input.SetLanguageCode("en")
	input.SetText(sentence)
	result, err := client.DetectSentiment(input)
	if err != nil {
		return SentimentResult{}, err
	}
	return SentimentResult{sentence, surroundingSentences, result.SentimentScore.Negative}, nil
}
