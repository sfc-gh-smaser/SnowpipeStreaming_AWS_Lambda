# SnowpipeStreaming_AWS_Lambda

This provides an initial example of using Snowpipe Streaming within a Lambda Function

##Setup
1.  Create Lambda Function (Java) using
- SnowpipeStreamingSample.jar
- Handler snowflake.aws.lambda.ToSnowflakeEventHandler::handleRequest

3.  Create two Layers and add to Function
- GSON (use archive 4_Layers/GSON.zip)
- Snowpipe_Streaming (use archive 4_Layers/SnowpipeStreaming1_0.zip)

4.  Add Environment Variables
- account
- database
- schema
- private_key
- warehouse
- user
- role
- table
- debug

5.  Set Timeout to a larger value (start with at least 30 seconds)

