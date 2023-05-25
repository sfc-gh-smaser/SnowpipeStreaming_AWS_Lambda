#!/bin/sh
 javac -cp lib2/aws-lambda-java-core-1.2.2.jar:lib2/aws-java-sdk-lambda-1.12.472.jar:lib2/com.google.gson-2.8.2.v20180104-1110.jar:classes:lib/* -d classes src/snowflake/aws/lambda/*.java

jar cf SnowpipeStreamingSample.jar -C classes snowflake src

