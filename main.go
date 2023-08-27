package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Row map[string]interface{}

type LambdaEvent struct {
	InputPath  string `json:"inputPath"`
	OutputPath string `json:"outputPath"`
	BucketName string `json:"bucketName"`
	FileType   string `json:"fileType"` // "csv" or "jsonl"
}

func HandleRequest(req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var event LambdaEvent
	err := json.Unmarshal([]byte(req.Body), &event)
	if err != nil {
		return events.APIGatewayProxyResponse{StatusCode: 400, Body: "Bad Request"}, nil
	}

	sess := session.Must(session.NewSession())
	s3Client := s3.New(sess)

	// Read from S3
	resp, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(event.BucketName),
		Key:    aws.String(event.InputPath),
	})
	if err != nil {
		log.Fatal(err)
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)

	if event.FileType == "csv" {
		processCSV(buf.String(), event.OutputPath, event.BucketName, s3Client)
	} else if event.FileType == "jsonl" {
		processJSONL(buf.String(), event.OutputPath, event.BucketName, s3Client)
	} else {
		log.Fatal("Unknown file type")
	}

	return events.APIGatewayProxyResponse{StatusCode: 200, Body: "OK"}, nil
}

func processCSV(content, outputPath, bucketName string, s3Client *s3.S3) {
	reader := csv.NewReader(strings.NewReader(content))
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	headers := records[0]
	var processedRecords [][]string
	processedRecords = append(processedRecords, append(headers, "_error", "has_error"))

	for _, record := range records[1:] {
		row := make(Row)
		var errorMap map[string]string
		hasError := false

		for i, header := range headers {
			if err := inferTypeAndSet(record[i], header, &row); err != nil {
				if errorMap == nil {
					errorMap = make(map[string]string)
				}
				errorMap[header] = err.Error()
				hasError = true
			}
		}

		errorStr, _ := json.Marshal(errorMap)
		processedRecords = append(processedRecords, append(record, string(errorStr), fmt.Sprintf("%v", hasError)))
	}

	// Write back to S3
	buf := &bytes.Buffer{}
	writer := csv.NewWriter(buf)
	writer.WriteAll(processedRecords)

	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(outputPath),
		Body:   bytes.NewReader(buf.Bytes()),
	})

	if err != nil {
		log.Fatal(err)
	}
}

func processJSONL(content, outputPath, bucketName string, s3Client *s3.S3) {
	lines := strings.Split(content, "\n")
	var processedLines []string

	for _, line := range lines {
		var row Row
		var errorMap map[string]string
		hasError := false

		if err := json.Unmarshal([]byte(line), &row); err != nil {
			log.Fatal("Error parsing JSONL: ", err)
		}

		for key, value := range row {
			strValue, ok := value.(string)
			if !ok {
				strValue = fmt.Sprintf("%v", value)
			}

			if err := inferTypeAndSet(strValue, key, &row); err != nil {
				if errorMap == nil {
					errorMap = make(map[string]string)
				}
				errorMap[key] = err.Error()
				hasError = true
			}
		}

		row["_error"] = errorMap
		row["has_error"] = hasError

		newLine, _ := json.Marshal(row)
		processedLines = append(processedLines, string(newLine))
	}

	// Write back to S3
	buf := &bytes.Buffer{}
	buf.WriteString(strings.Join(processedLines, "\n"))

	_, err := s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(outputPath),
		Body:   bytes.NewReader(buf.Bytes()),
	})

	if err != nil {
		log.Fatal(err)
	}
}

func inferTypeAndSet(value string, key string, row *Row) error {
	if i, err := strconv.Atoi(value); err == nil {
		(*row)[key] = i
		return nil
	}
	if f, err := strconv.ParseFloat(value, 64); err == nil {
		(*row)[key] = f
		return nil
	}
	if value == "true" || value == "false" {
		(*row)[key] = (value == "true")
		return nil
	}
	if value != "" {
		(*row)[key] = value
		return nil
	}

	return fmt.Errorf("Not inferable")
}

func main() {
	lambda.Start(HandleRequest)
}
