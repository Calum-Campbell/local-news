package internal

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func getFirstFileFromTarGzip(b []byte) ([]byte, error) {
	var out []byte

	gr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return out, err
	}

	tr := tar.NewReader(gr)
	tr.Next()

	out, err = ioutil.ReadAll(tr)
	if err != nil {
		return out, err
	}

	return out, nil
}

func GetTextBytes(session *session.Session, fileName string) ([]byte, error) {
	var bytesArray []byte
	item := fileName
	bucket := "whatif-local-news-le"
	writer := aws.NewWriteAtBuffer([]byte{})
	downloader := s3manager.NewDownloader(session)

	log.Println("Downloading text file")
	_, err := downloader.Download(writer,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(item),
		})

	if err != nil {
		return bytesArray, err
	}
	bytesArray = writer.Bytes()
	return bytesArray, nil
}
