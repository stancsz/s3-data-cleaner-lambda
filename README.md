# S3 Data Cleaner Lambda

## Overview
S3 Data Cleaner Lambda is an AWS Lambda function written in Go designed to clean and validate data files stored in Amazon S3. The service works with both CSV and JSONL file formats, automatically infers data types for each field, and handles errors gracefully. The cleaned-up output is written back to an S3 bucket.

## Features
- File Format Support: Supports both CSV and JSONL formats.
- Data Type Inference: Automatically infers data types for each field.
- Error Handling: Rows with parsing issues are flagged, and error details are stored in special columns (_error and has_error).

## Requirements
- Go 1.16 or higher
- AWS CLI configured with appropriate permissions
- AWS Lambda Go SDK
- AWS SDK for Go

## Local Setup
### Clone the Repository
```bash
git clone https://github.com/stancsz/s3-data-cleaner-lambda.git
```

### Install Dependencies
Navigate to the project directory and download the dependencies:
```bash
cd s3-data-cleaner-lambda
go mod tidy
```

### Build the Project
Compile the Go code to create a binary:
```bash
GOOS=linux go build -o main
```

### Package the Lambda Function
Package the binary into a ZIP file:
```bash
zip function.zip main
```

## AWS Lambda Deployment
### Create an Execution Role
1. Navigate to the AWS IAM console and create a new role.
2. Attach the AWSLambdaExecute policy to the role.

### Deploy Using AWS CLI
Upload the ZIP file to create a new Lambda function:
```bash
aws lambda create-function \
  --function-name S3DataCleanerLambda \
  --zip-file fileb://function.zip \
  --handler main \
  --runtime go1.x \
  --role arn:aws:iam::[your-account-id]:role/[your-execution-role]
```
Replace `[your-account-id]` and `[your-execution-role]` with your AWS account ID and the execution role you created.

### Update Function (if needed)
To update the function code:
```bash
aws lambda update-function-code \
  --function-name S3DataCleanerLambda \
  --zip-file fileb://function.zip
```

## Usage
### Trigger Setup
You can set up an S3 trigger in the Lambda console to invoke this function whenever a new file is uploaded to a specific bucket.

### Input and Output
Specify input and output S3 paths, the S3 bucket name, and the file type (CSV or JSONL) as environment variables or directly in the Lambda function's configuration.

### Invoking Manually
To invoke the function manually, you can use:
```bash
aws lambda invoke \
  --function-name S3DataCleanerLambda \
  --payload '{"inputS3Path": "s3://input-bucket/file.csv", "outputS3Path": "s3://output-bucket/cleaned_file.csv", "fileType": "csv"}' \
  output.txt
```

## Contributing
Feel free to open issues or submit pull requests. Contributions are welcome!

## License
MIT