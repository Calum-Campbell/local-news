package internal

import (
	"log"

	"sort"
	"strings"

	"github.com/aws/aws-sdk-go/service/comprehend"
)

type SentimentResult struct {
	Sentence             string
	SurroundingSentences string
	NegativeSentiment    *float64
}

func AnalyseTextSentiment(client *comprehend.Comprehend, text string) ([]SentimentResult, error) {
	sentences := strings.Split(strings.TrimSpace(text), ". ")
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
	if len(sentimentArray) > 10 {
		return sentimentArray[0:10], nil
	} else {
		return sentimentArray, nil
	}
}

func AnalyseSentenceSentiment(client *comprehend.Comprehend, sentence string, surroundingSentences string) (SentimentResult, error) {
	input := comprehend.DetectSentimentInput{}
	input.SetLanguageCode("en")
	input.SetText(sentence)
	result, err := client.DetectSentiment(&input)
	if err != nil {
		return SentimentResult{}, err
	}
	return SentimentResult{sentence, surroundingSentences, result.SentimentScore.Negative}, nil
}
