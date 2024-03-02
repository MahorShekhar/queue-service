package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class RedisQueueService implements QueueService {

  private final String redisEndpointUrl;
  private final String redisApiToken;

  private static final String REDIS_ZADD_COMMAND = "zadd";
  private static final String REDIS_ZREM_COMMAND = "zadd";
  private static final String REDIS_ZPOPMAX_COMMAND = "zpopmax";


  RedisQueueService() {
    String propFileName = "config.properties";
    Properties confInfo = new Properties();

    try (InputStream inStream = getClass().getClassLoader().getResourceAsStream(propFileName)) {
      confInfo.load(inStream);
    } catch (IOException e) {
      e.printStackTrace();
    }

    this.redisEndpointUrl = confInfo.getProperty("redisEndpointUrl");
    this.redisApiToken = confInfo.getProperty("redisApiToken");
  }

  @Override
  public void push(String queueUrl, String msgBody) {
      int priority = extractPriorityFromJson(msgBody);
      makePostRequest(redisEndpointUrl + "/"+ REDIS_ZADD_COMMAND + "/" + queueUrl + "/" + priority, msgBody);
  }

  @Override
  public Message pull(String queueUrl) {

    String redisResponse = makeGetRequest(redisEndpointUrl + "/"+ REDIS_ZPOPMAX_COMMAND+ "/"+ queueUrl);

    return createMessageFromRedisResponse(redisResponse);
  }

  @Override
  public void delete(String queueUrl, String receiptId) {
    // pull method is deleting the message as well
  }

  private int extractPriorityFromJson(String msgBody) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode rootNode = mapper.readTree(msgBody);
      JsonNode priorityNode = rootNode.get("priority");
      if (priorityNode != null && priorityNode.isInt()) {
        return priorityNode.intValue();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return 0; // Default priority if not found or error occurred
  }

  // this method creates a Message object from response received from redis.
  private Message createMessageFromRedisResponse(String response) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode rootNode = mapper.readTree(response);
      JsonNode resultNode = rootNode.get("result");

      if (resultNode != null && resultNode.isArray() && resultNode.size() > 0) {
        JsonNode messageNode = mapper.readTree(resultNode.get(0).asText());

        int priority = messageNode.has("priority") ? messageNode.get("priority").asInt() : 0;
        String messageBody = messageNode.has("content") ? messageNode.get("content").asText() : "";

        return new Message(messageBody, UUID.randomUUID().toString(), priority);
      } else {
        // Queue is empty or no valid message found
        return null;
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse JSON response", e);
    }
  }

  private String makeGetRequest(String apiUrl) {
    CloseableHttpClient client = HttpClients.createDefault();

    HttpGet httpGet = new HttpGet(apiUrl);
    httpGet.addHeader("Content-Type", "application/json");
    httpGet.addHeader("Authorization", String.format("Bearer %s", redisApiToken));

    try {
      CloseableHttpResponse response = client.execute(httpGet);

      if (response.getStatusLine().getStatusCode() != 200) {
        throw new RuntimeException(String.format("Redis Api failed with status %s", response.getStatusLine().getStatusCode()));
      }
      return EntityUtils.toString(response.getEntity());
    } catch (Exception e) {
      throw new RuntimeException("Unable to call Redis api", e);
    }
  }

  private String makePostRequest(String apiUrl, String msgBody) {
    CloseableHttpClient client = HttpClients.createDefault();

    HttpPost httpPost = new HttpPost(apiUrl);

    try {
      httpPost.setEntity(new StringEntity(msgBody));
    } catch (Exception e) {
      throw new RuntimeException("Unable to call Redis api", e);
    }

    httpPost.addHeader("Content-Type", "application/json");
    httpPost.addHeader("Authorization", String.format("Bearer %s", redisApiToken));

    try {
      CloseableHttpResponse response = client.execute(httpPost);

      if (response.getStatusLine().getStatusCode() != 200) {
        throw new RuntimeException(String.format("Redis Api failed with status %s", response.getStatusLine().getStatusCode()));
      }
      return EntityUtils.toString(response.getEntity());
    } catch (Exception e) {
      throw new RuntimeException("Unable to call Redis api", e);
    }
  }
}
