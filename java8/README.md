# Spark API Code Sample In Java

This Sample project is built with Java 8 + Maven.

`Apache HttpClient` and `Gson` are used to simplify the codebase.

## Usage

**An OAuth2 client is required.**

Please refer to https://app.sparkcommodities.com/freight/data-integrations/api if you 
don't have any.

**Steps**:

 1. Set environment variables: `SPARK_CLIENT_ID` and `SPARK_CLIENT_SECRET`
 2. Simply run the `main` method in `SparkAPISampleApp`, or `mvn compile exec:java -Dexec.mainClass="com.sparkcommodities.api.SparkAPISampleApp"`
