#!/bin/bash

if [[ $# -eq 0 ]] ; then
    echo 'Please enter your bucket name as ./setup_infra.sh your-bucket'
    exit 0
fi

AWS_ID=$(aws sts get-caller-identity --query Account --output text | cat)
AWS_REGION=$(aws configure get region)

echo "Creating local config files"

echo '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "logs:PutLogEvents",
                "logs:CreateLogGroup",
                "logs:CreateLogStream"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": "arn:aws:s3:::'$1'/*"
        }
    ]
}' > ./policy

echo '{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}' > ./trust-policy.json

echo '[
  {
    "Id": "1",
    "Arn": "arn:aws:lambda:'$AWS_REGION':'$AWS_ID':function:dataPull"
  }
]' > ./targets.json

echo "Packaging local lambda_function.py"
cd dataPull
zip -r ../myDeploymentPackage.zip .
cd ..

echo "Creating bucket "$1" in region $AWS_REGION"
aws s3api create-bucket --bucket $1 --region $AWS_REGION --create-bucket-configuration LocationConstraint=$AWS_REGION --output text > setup.log

echo "Creating Policy if it doesn't exist"
aws iam create-policy --policy-name AWSLambdaS3Policy --policy-document file://policy --output text 2>/dev/null >> setup.log || echo "Policy AWSLambdaS3Policy already exists"

echo "Creating Role if it doesn't exist"
aws iam create-role --role-name lambda-s3-role --assume-role-policy-document file://trust-policy.json --output text 2>/dev/null >> setup.log || echo "Role lambda-s3-role already exists"

echo "Attaching Policy to Role"
aws iam attach-role-policy --role-name lambda-s3-role --policy-arn arn:aws:iam::$AWS_ID:policy/AWSLambdaS3Policy --output text >> setup.log

echo "Sleeping 10 seconds to allow policy to attach to role"
sleep 10

echo "Creating Lambda function"
# Check if the function already exists before creating it
if ! aws lambda get-function --function-name dataPull 2>/dev/null; then
    aws lambda create-function --function-name dataPull --runtime python3.8 --role  arn:aws:iam::$AWS_ID:role/lambda-s3-role --handler lambda_function.lambda_handler --zip-file fileb://myDeploymentPackage.zip  --timeout 60 --output text >> setup.log
else
    echo "Function dataPull already exists"
fi

echo "Creating cloudwatch rule to schedule lambda every 5 minutes"
aws events put-rule --name my-scheduled-rule --schedule-expression 'rate(5 minutes)' --output text >> setup.log

echo "Attaching lambda function to event and then to the rule"
# Generate a unique statement ID to avoid conflicts
STATEMENT_ID="my-scheduled-event-$(date +%s)"
aws lambda add-permission --function-name dataPull --statement-id $STATEMENT_ID --action 'lambda:InvokeFunction' --principal events.amazonaws.com --source-arn arn:aws:events:$AWS_REGION:$AWS_ID:rule/my-scheduled-rule --output text >> setup.log || echo "Statement ID $STATEMENT_ID already exists"
aws events put-targets --rule my-scheduled-rule --targets file://targets.json --output text >> setup.log

echo "Done"