You cannot do backup and restore AWS ElasticSearch to other domain so I customize aws source code to help you create snapshot/restore with AWS ElasticSearch.

## Below is how to deploy to AWS with source code + python library.
### Required libraries:
    ```
    python3 -m pip install elasticsearch-curator --target .
    python3 -m pip install AWS4Auth --target .
    ```
    
### Create Zip file to upload:
https://aws.amazon.com/premiumsupport/knowledge-center/build-python-lambda-deployment-package/   

1. Add permissions (Linux and macOS only)   
  ``
  chmod -R 755 .
  ``
2. Manually build a deployment package   
  ``
  zip -r ../lambda_function.zip .
  ``
3. Upload to AWS Lambda function

4. Add environment variables to lambda:
AWS_CURRENT_REGION=ap-northeast-1   
AWS_ES_HOST_NAME=https://xxxxxx/   
AWS_S3_BUCKET_NAME=bucket_name   
// with my sample, AWS_ROLE_ARN should be ARN of 'arn:aws:iam::777777777777:role/Elasticsearch_Role'
AWS_ROLE_ARN=IAM_ROLE_ARN      
ES_EXPECTED_OPERATION=take_snapshot   

## Needed 2 IAM role:
#### 1. For lambda: (e.g. name: backup_Elasticsearch_lambda_role) 
Lambda will runs as this role to call ElasticSearch and pass the Elasticsearch_Role role to elasticSearch.
```
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "iam:PassRole",
                "Resource": "arn:aws:iam::777777777777:role/Elasticsearch_Role"
            },
            {
                "Effect": "Allow",
                "Action": "es:ESHttpPut",
                "Resource": "arn:aws:es:ap-southeast-1:777777777777:domain/ES_NAME/*"
            }
        ]
    }
```


#### 2. For ElasticSearch: (e.g. name: Elasticsearch_Role)
ElasticSearch will use this passed role to put_get the backup files to_from S3. 
```
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": [
                    "s3:ListBucket"
                ],
                "Effect": "Allow",
                "Resource": [
                    "arn:aws:s3:::S3_BACKET_NAME_TO_STORE_SNAPSHOT"
                ]
            },
            {
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject"
                ],
                "Effect": "Allow",
                "Resource": [
                    "arn:aws:s3:::S3_BACKET_NAME_TO_STORE_SNAPSHOT/*"
                ]
            }
        ]
    }
```