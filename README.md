# SnowpipeStreaming_AWS_Lambda

This provides an initial example of using Snowpipe Streaming within a Lambda Function

## Setup
1.  Create Lambda Function (Java) using
- SnowpipeStreamingSample.jar
- Handler snowflake.aws.lambda.ToSnowflakeEventHandler::handleRequest

2.  Create two Layers and add to Function
- GSON (use archive 4_Layers/GSON.zip)
- Snowpipe_Streaming (use archive 4_Layers/SnowpipeStreaming1_0.zip)

3.  Add Environment Variables
- account
- database
- schema
- private_key
- warehouse
- user
- role
- table
- debug (optional)

4.  Set additional configuration settings
- Timeout to a larger value (start with at least 30 seconds)

