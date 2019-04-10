package internal

import (
	"encoding/json"
	"log"
	"time"

	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/comprehend"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type Entity struct {
	Text string
	Type string
}

type EntityApiResult struct {
	Entities []Entity
}

type TypedEntityResult struct {
	People        []Entity
	Places        []Entity
	Dates         []Entity
	Organisations []Entity
}

func StartEntitiesJob(client *comprehend.Comprehend, fileName string) (*string, error) {
	var jobId *string
	inputConfig := comprehend.InputDataConfig{}
	inputConfig.SetInputFormat("ONE_DOC_PER_FILE")
	inputConfig.SetS3Uri("s3://lauren-temp/" + fileName)

	outputConfig := comprehend.OutputDataConfig{}
	outputConfig.SetS3Uri("s3://lauren-temp/entities")

	entityJobInput := comprehend.StartEntitiesDetectionJobInput{}
	entityJobInput.SetLanguageCode("en")
	entityJobInput.SetDataAccessRoleArn("arn:aws:iam::942464564246:role/comprehend-s3-access")
	entityJobInput.SetInputDataConfig(&inputConfig)
	entityJobInput.SetOutputDataConfig(&outputConfig)

	submittedJob, err := client.StartEntitiesDetectionJob(&entityJobInput)
	if err != nil {
		return jobId, err
	}
	jobId = submittedJob.JobId

	return jobId, nil
}

func GetEntitiesFileOutputPath(client *comprehend.Comprehend, jobId *string) (*string, error) {
	var outputPath *string
	describeInput := comprehend.DescribeEntitiesDetectionJobInput{
		JobId: jobId,
	}
	for {
		time.Sleep(10 * time.Second)
		res, err := client.DescribeEntitiesDetectionJob(&describeInput)
		if err != nil {
			return outputPath, err
		}
		log.Print("Entities analysis: ")
		log.Println(*res.EntitiesDetectionJobProperties.JobStatus)
		if *res.EntitiesDetectionJobProperties.JobStatus == "COMPLETED" {
			outputPath = res.EntitiesDetectionJobProperties.OutputDataConfig.S3Uri
			break
		}
	}
	return outputPath, nil
}

func EntityFileToJson(outputPath string, session *session.Session) ([]Entity, error) {
	var entitiesArray []Entity
	var dat EntityApiResult
	outputId := strings.Split(outputPath, "/")[4]
	item := "entities/" + outputId + "/output/output.tar.gz"
	bucket := "lauren-temp"

	writer := aws.NewWriteAtBuffer([]byte{})
	downloader := s3manager.NewDownloader(session)

	log.Printf("Downloading entity file from S3 bucket: %s", bucket)

	_, err := downloader.Download(writer,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(item),
		})

	if err != nil {
		return entitiesArray, err
	}

	content, err := getFirstFileFromTarGzip(writer.Bytes())
	if err != nil {
		return entitiesArray, err
	}

	json.Unmarshal(content, &dat)
	entitiesArray = dat.Entities
	return entitiesArray, nil
}

func AddEntityIfUnique(typeArray []Entity, entity Entity) []Entity {
	for _, i := range typeArray {
		if i.Text == entity.Text {
			return typeArray
		}
	}
	return append(typeArray, entity)
}

func AnalyseEntities(entityArray []Entity) TypedEntityResult {
	var people []Entity
	var places []Entity
	var dates []Entity
	var organisations []Entity

	log.Println("Structuring entity data")

	for _, entity := range entityArray {
		switch entityType := entity.Type; entityType {
		case "PERSON":
			people = AddEntityIfUnique(people, Entity{Text: entity.Text, Type: entity.Type})
		case "LOCATION":
			places = AddEntityIfUnique(places, Entity{Text: entity.Text, Type: entity.Type})
		case "DATE":
			dates = AddEntityIfUnique(dates, Entity{Text: entity.Text, Type: entity.Type})
		case "ORGANIZATION":
			organisations = AddEntityIfUnique(organisations, Entity{Text: entity.Text, Type: entity.Type})
		}
	}
	return TypedEntityResult{People: people, Places: places, Dates: dates, Organisations: organisations}
}
