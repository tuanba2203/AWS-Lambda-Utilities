This is the customized version of LogsToElasticsearch from AWS. ref: https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_ES_Stream.html
I just modified alittle for match with my requirement.
With this modification, you can push log from cloudwatch log stream to many diff elasticsearch's indexs just with one lambda source code.
You can see the modification from line 67 to line 73.
This lambda will push log from log stream to your ES with index maned pattern: [YYYY]_[MM]_[CLOUDWATCH-LOG-GROUP-NAME]_log_cw_log_indx
