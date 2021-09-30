# Python Exercise Solution
This repository contains the solution to the Python exercise which regards processing CSV files uploaded to an S3 bucket.

## Expected Resources
Find below an outline of resources required to be created that is supposed to be within `resources.yml`.
The current contents is an export of the AWS SAM definition for a sample function used to test the function
locally.

### Lambda Function
- Trigger when an object is created in a specific S3 bucket
- Trigger function: `handler.handler`
- Amazon CloudWatch Logs permissions:
  - Resource:
  - Permissions:
    - Allow: logs:CreateLogGroup
    - Allow: logs:CreateLogStream
    - Allow: logs:PutLogEvents
  - Environment Variable:
    - DB_BUCKET: <name-of-output-bucket>
- Amazon S3
  - Resource: arn:aws:s3:::*
  - Permissions:
    - Allow: s3:GetObject
    - Allow: s3:PutObject

### S3 Butcket
Two S3 buckets are required. One for the file uploads which triggers the lambda function. The
other S3 Bucket will be used to house the database created from processing uploaded files.
