package com.sparkcommodities.api;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

class SparkClient {
    private static final String API_BASE_URL = "https://api.sparkcommodities.com";
    private final String clientId;
    private final String clientSecret;

    private String accessToken;
    private Gson gson = new Gson();

    public SparkClient(String clientId, String clientSecret) {
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    /**
     * Get a new accessToken. Access tokens are the thing that applications use to make API requests.
     * Access tokens must be kept confidential in storage.
     * <p>
     * # Procedure:
     * Do a POST query with `grant_type` and `scope` in the body. A basic authorization
     * HTTP header is required. The "Basic" HTTP authentication scheme is defined in
     * RFC 7617, which transmits credentials as `client_id:client_secret` pairs, encoded
     * using base64.
     *
     * @throws IOException
     */
    public void getAccessToken() throws IOException {
        String urlAuth = API_BASE_URL + "/oauth/token/";

        HttpClient httpClient = HttpClients.createDefault();

        String authHeader = Base64.getEncoder().encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));
        StringEntity authParams = new StringEntity("{\"grantType\": \"clientCredentials\", \"scopes\": \"read:lng-freight-prices\"}");

        HttpUriRequest request = RequestBuilder.post().setUri(urlAuth)
                .setHeader(HttpHeaders.AUTHORIZATION, authHeader)
                .setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType())
                .setEntity(authParams)
                .build();

        HttpResponse httpResponse = httpClient.execute(request);
        String responseString = EntityUtils.toString(httpResponse.getEntity(), StandardCharsets.UTF_8);
        JsonObject responseJson = gson.fromJson(responseString, JsonObject.class);
        accessToken = responseJson.get("accessToken").getAsString();
        int expiresInSeconds = responseJson.get("expiresIn").getAsInt();

        System.out.println("Successfully fetched an access token " + accessToken + ", expires in " + expiresInSeconds + " seconds");
    }


    /**
     * List available contract names.
     *
     * @throws IOException
     */
    public void listContracts() throws IOException {
        String urlContracts = API_BASE_URL + "/v1.0/contracts/";

        HttpClient httpClient = HttpClients.createDefault();

        HttpUriRequest request = RequestBuilder.get().setUri(urlContracts)
                .setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken)
                .setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType())
                .build();

        HttpResponse httpResponse = httpClient.execute(request);
        String responseString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
        JsonObject responseJson = gson.fromJson(responseString, JsonObject.class);

        for (JsonElement jsonElement : responseJson.get("data").getAsJsonArray()) {
            JsonObject contractJson = jsonElement.getAsJsonObject();
            String contractName = contractJson.get("fullName").getAsString();
            System.out.println("contract: " + contractName);
        }
    }
}


public class SparkAPISampleApp {
    public static void main(String[] args) throws IOException {
        String sparkClientId = System.getenv("SPARK_CLIENT_ID");
        String sparkClientSecret = System.getenv("SPARK_CLIENT_SECRET");
        SparkClient sparkClient = new SparkClient(sparkClientId, sparkClientSecret);

        sparkClient.getAccessToken();
        sparkClient.listContracts();
    }
}
