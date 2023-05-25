package snowflake.aws.lambda;
/*
* This sample Lambda prototype showing a way to send events to Snowflake using AWS Lambda.  Done by converging these two samples:
* Snowflake Sample:  https://github.com/snowflakedb/snowflake-ingest-java/blob/master/src/main/java/net/snowflake/ingest/streaming/example/SnowflakeStreamingIngestExample.java
* AWS Sample:  https://docs.aws.amazon.com/lambda/latest/dg/java-handler.html
* For higher-volume, Data Ingestion use cases, consider using the Snowpipe API directly or Kinesis/Kafka/MSK??
* API Docs:  https://javadoc.io/doc/net.snowflake/snowflake-ingest-sdk/latest/
*/

import java.util.Properties;
import java.util.HashMap;
import java.util.Map;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;

public class ToSnowflakeEventHandler implements RequestHandler<Map<String,String>, String>{
    private Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private JsonParser gsonParser = new JsonParser();
    private static final Logger LOG = LoggerFactory.getLogger(ToSnowflakeEventHandler.class);
    private static boolean DEBUG=false;
    private SnowflakeStreamingIngestClient CLIENT=null;
    private String CLIENTID=null;
    private SnowflakeStreamingIngestChannel CHAN=null;
    private String INIT_ERROR=null;
    private int ID=1;

    // entire event Map creates one row in Snowflake
    @Override
    public String handleRequest(Map<String,String> event, Context context) {
        String response = new String("200 OK");
        if(CLIENTID==null) CLIENTID=context.getAwsRequestId();
        if(CLIENT==null ||CLIENT.isClosed()) CLIENT=getClient();  // ensures unique name for client as lambdas can be running in parallel
        if(CHAN==null ||!CHAN.isValid() ||CHAN.isClosed()) CHAN=getChannel(CLIENT);
        if(CHAN==null) return new String("500 Unable to Initialize SF Streaming Client, check Lambda configuration");
        if(DEBUG) context.getLogger().log("EVENT: " + gson.toJson(event));
        try{
            if(INIT_ERROR!=null) throw new Exception("INIT ERROR:  "+INIT_ERROR);
            //process single row event
            Map<String, Object> row = new HashMap<>();
            row.put("ENV",gson.toJson(System.getenv()));
            row.put("CONTEXT",gson.toJson(System.getenv()));
            row.put("EVENT",gson.toJson(event));
            row.put("EVENT_TYPE",event.getClass().toString());
            InsertValidationResponse resp = CHAN.insertRow(row, String.valueOf(ID));
            if (resp.hasErrors()) throw resp.getInsertErrors().get(0).getException();
            else {
                String error_msg=checkCommitError(ID,CHAN);
                if (error_msg!=null) response=new String ("500 "+error_msg); // not throwing exception, returning 500 status
                else ID++;
            }
        }
        catch (Exception ex){
            LOG.error(ex.getMessage());
            response=new String("500 " +ex.getMessage());
        }
        return response;
    }

    // each Map entry creates one row in Snowflake (List??)
    public String handleRequestAsRows(Map<String,String> event, Context context) {
        String response = new String("200 OK");
        if(CLIENTID==null) CLIENTID=context.getAwsRequestId();
        if(CLIENT==null || CLIENT.isClosed()) CLIENT=getClient();
        if(CHAN==null ||!CHAN.isValid() ||CHAN.isClosed()) CHAN=getChannel(CLIENT);
        if(CHAN==null) return new String("500 Unable to Initialize SF Streaming Client, check Lambda configuration");
        if(DEBUG) context.getLogger().log("EVENT: " + gson.toJson(event));
        InsertValidationResponse resp =null;
        try{
            if(INIT_ERROR!=null) throw new Exception("INIT ERROR:  "+INIT_ERROR);
            for( String key : event.keySet() ){
                String value = (String) event.get(key);
                value=gsonParser.parse(value).toString();
                if(DEBUG) context.getLogger().log("EVENT_ROW: " + value);
                //process a Map of rows within event
                Map<String, Object> row = new HashMap<>();
                row.put("ENV",gson.toJson(System.getenv()));
                row.put("CONTEXT",gson.toJson(System.getenv()));
                row.put("EVENT",value);
                row.put("EVENT_TYPE",key);
                resp = CHAN.insertRow(row, String.valueOf(ID));
            }
            if (resp==null || resp.hasErrors()) throw resp.getInsertErrors().get(0).getException();
            else {
                 String error_msg=checkCommitError(ID,CHAN);
                if (error_msg!=null) return new String ("500 "+error_msg);  // not throwing exception, returning 500 status and not continuing
                else ID++;
            }
        }
        catch (Exception ex){
            LOG.error(ex.getMessage());
            response=new String("500 " +ex.getMessage());
        }
        return response;
    }

    private SnowflakeStreamingIngestClient getClient() {
        Properties p = new Properties();
        p.setProperty("scheme","https");
        p.setProperty("port","443");
        p.setProperty("host",System.getenv("account")+".snowflakecomputing.com");
        p.setProperty("private_key",System.getenv("private_key"));
        p.setProperty("account",System.getenv("account"));
        p.setProperty("role",System.getenv("role"));
        p.setProperty("user",System.getenv("user"));
        p.setProperty("warehouse",System.getenv("warehouse"));
        SnowflakeStreamingIngestClient client = SnowflakeStreamingIngestClientFactory.builder(CLIENTID).setProperties(p).build();
        return client;
    }

    private SnowflakeStreamingIngestChannel getChannel(SnowflakeStreamingIngestClient client) {
        if(client==null) getClient();
        OpenChannelRequest req =
                OpenChannelRequest.builder("AWS_CHANNEL") // hard coding channel name, as client is unique for each lambda instance
                        .setDBName(System.getenv("database"))
                        .setSchemaName(System.getenv("schema"))
                        .setTableName(System.getenv("table"))
                        .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE) // continue client if error
                        .build();
        return client.openChannel(req);
    }

    private String checkCommitError(int id,SnowflakeStreamingIngestChannel chan) throws Exception {
        String msg=null;
        int retryCount = 0;
        int maxRetries = 20;
        String offsetTokenFromSnowflake;
        String expectedOffsetTokenInSnowflake = String.valueOf(id);
        for (offsetTokenFromSnowflake = chan.getLatestCommittedOffsetToken(); offsetTokenFromSnowflake == null
                || !offsetTokenFromSnowflake.equals(expectedOffsetTokenInSnowflake); ) {
            Thread.sleep(1000);
            offsetTokenFromSnowflake = chan.getLatestCommittedOffsetToken();
            retryCount++;
            if (retryCount >= maxRetries) {
                    msg=String.format(
                        "Failed to receive required OffsetToken in Snowflake:%s after MaxRetryCounts:%s (%S) at ID=%d",
                        expectedOffsetTokenInSnowflake, maxRetries, offsetTokenFromSnowflake,id);
            }
        }
        return msg;
    }
    
    static {
        DEBUG=(System.getenv("debug")!=null?Boolean.valueOf(System.getenv("debug")):false);
        if(!DEBUG) System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "ERROR");
    }
}
/*  INITIAL SETUP INSTRUCTIONS
1. Create Key Pair Files:
       from a Command Line:
            openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
            openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
2.  Compile and package this java file into Lambda-compatible JAR
3. In Snowflake (using a Worksheet, or other):
 a) create Service Account:
    use role ACCOUNTADMIN;
    create role if not exists STREAMING_AGENT;
    create or replace user aws_streaming1 COMMENT="Creating for VHOL";
    alter user aws_streaming1 set rsa_public_key='<Paste Your Public Key Here>';
 b) Create Landing Table (using VARCHAR till we see format and review fields, will change to VARIANT)
    create or replace table MYDB.MYSCHEMA.AWS_STREAMING_TABLE(ENV variant,CONTEXT variant,EVENT variant,EVENT_TYPE varchar);
 c) Grant Access:
    grant role STREAMING_AGENT to user aws_streaming1;
    grant usage on warehouse MYWAREHOUSE to role STREAMING_AGENT;
    grant usage on database MYDB to role STREAMING_AGENT;
    grant usage on schema MYSCHEMA to role STREAMING_AGENT;
    grant insert on table MYDB.MYSCHEMA.AWS_STREAMING_TABLE to role STREAMING_AGENT;
3.  In AWS configure Lambda:
	a) Include Adding Properties:
	    property 'private_key' setting value from file 'rsa_key.p8' without header/footer or carriage returns
	    property database=MYDB
	    property schema=MYSCHEMA
	    property table=AWS_STREAMING_TABLE
	    property user=aws_streaming1
	    property role=STREAMING_AGENT
	    property account=MY_ACCOUNT
	    property warehouse=MYWAREHOUSE
	 b) Add dependant JAR files (in layers, specifically GSON and those required for Snowpipe Streaming)  https://github.com/snowflakedb/snowflake-ingest-java/
	 c) Configure Lambda to have network access to Snowflake, if needed
4.  Test Lambda function
5.  See new Records in Snowflake
    select * from MYDB.MYSCHEMA.AWS_STREAMING_TABLE;
6.  Configure a Trigger to fire Lambda as appropriate and test
7.  Recreate Snowflake Table to minimize unneeded data and use VARIANT datatypes
8.  Create a second Lambda function to process a batch of records within a single event payload
*/


/*
// sample payload for handleRequest
{
  "key1": "value1",
  "key2": "value2",
  "key3": "value3"
}


// sample payload to test handleRequestAsRows method (multi-row event)
{
  "key1": "{\"value\":1}",
  "key2": "{\"value\":2}",
  "key3": "{\"value\":3}"
}

 */