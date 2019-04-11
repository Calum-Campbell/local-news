package internal

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/comprehend"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type KeyPhrase struct {
	Text string
}

type KeyPhraseApiResult struct {
	KeyPhrases []KeyPhrase
}

func SmallerFileKeyPhraseAnalysis(text string, client *comprehend.Comprehend) ([]KeyPhrase, error) {
	var input comprehend.BatchDetectKeyPhrasesInput
	var keyPhraseArray []KeyPhrase
	input.SetLanguageCode("en")
	input.SetTextList([]*string{aws.String(text)})

	output, err := client.BatchDetectKeyPhrases(&input)
	if err != nil {
		return keyPhraseArray, err
	}
	for _, keyPhrase := range output.ResultList[0].KeyPhrases {
		keyPhraseArray = append(keyPhraseArray, KeyPhrase{Text: *keyPhrase.Text})
	}
	return keyPhraseArray, nil
}

func StartKeyPhrasesJob(client *comprehend.Comprehend, fileName string) (*string, error) {
	var jobId *string
	inputConfig := comprehend.InputDataConfig{}
	inputConfig.SetInputFormat("ONE_DOC_PER_FILE")
	inputConfig.SetS3Uri("s3://lauren-temp/" + fileName)

	outputConfig := comprehend.OutputDataConfig{}
	outputConfig.SetS3Uri("s3://lauren-temp/key-phrases")

	keyPhrasesJobInput := comprehend.StartKeyPhrasesDetectionJobInput{}
	keyPhrasesJobInput.SetLanguageCode("en")
	keyPhrasesJobInput.SetDataAccessRoleArn("arn:aws:iam::942464564246:role/comprehend-s3-access")
	keyPhrasesJobInput.SetInputDataConfig(&inputConfig)
	keyPhrasesJobInput.SetOutputDataConfig(&outputConfig)

	submittedJob, err := client.StartKeyPhrasesDetectionJob(&keyPhrasesJobInput)
	if err != nil {
		return jobId, err
	}
	jobId = submittedJob.JobId

	return jobId, nil
}

func GetKeyPhrasesFileOutputPath(client *comprehend.Comprehend, jobId *string) (*string, error) {
	var outputPath *string
	describeInput := comprehend.DescribeKeyPhrasesDetectionJobInput{
		JobId: jobId,
	}
	for {
		time.Sleep(10 * time.Second)
		res, err := client.DescribeKeyPhrasesDetectionJob(&describeInput)
		if err != nil {
			return outputPath, err
		}
		log.Print("Key phrases analysis: ")
		log.Println(*res.KeyPhrasesDetectionJobProperties.JobStatus)
		if *res.KeyPhrasesDetectionJobProperties.JobStatus == "COMPLETED" {
			outputPath = res.KeyPhrasesDetectionJobProperties.OutputDataConfig.S3Uri
			break
		}
	}
	return outputPath, nil
}

func KeyPhrasesFileToJson(outputPath string, session *session.Session) ([]KeyPhrase, error) {
	var keyPhrasesArray []KeyPhrase
	var dat KeyPhraseApiResult
	outputId := strings.Split(outputPath, "/")[4]
	item := "key-phrases/" + outputId + "/output/output.tar.gz"
	bucket := "lauren-temp"

	writer := aws.NewWriteAtBuffer([]byte{})
	downloader := s3manager.NewDownloader(session)

	log.Printf("Downloading key phrase file from S3 bucket: %s", bucket)

	_, err := downloader.Download(writer,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(item),
		})

	if err != nil {
		return keyPhrasesArray, err
	}

	content, err := getFirstFileFromTarGzip(writer.Bytes())

	if err != nil {
		return keyPhrasesArray, err
	}

	json.Unmarshal(content, &dat)
	keyPhrasesArray = dat.KeyPhrases
	return keyPhrasesArray, nil
}

func AddKeyPhraseIfUnique(keyPhrasesArray []KeyPhrase, keyPhrase KeyPhrase) []KeyPhrase {
	for _, i := range keyPhrasesArray {
		if i.Text == keyPhrase.Text {
			return keyPhrasesArray
		}
	}
	return append(keyPhrasesArray, keyPhrase)
}

func AnalyseKeyPhrases(keyPhrases []KeyPhrase) []KeyPhrase {
	var uniqueKeyPhrases []KeyPhrase

	fmt.Println("Structuring key phrase data")
	for _, keyPhrase := range keyPhrases {
		uniqueKeyPhrases = AddKeyPhraseIfUnique(uniqueKeyPhrases, KeyPhrase{Text: keyPhrase.Text})
	}
	return uniqueKeyPhrases
}
