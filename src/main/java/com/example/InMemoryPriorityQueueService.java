package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class InMemoryPriorityQueueService implements QueueService {
  private final Map<String, Queue<Message>> queues;

  private long visibilityTimeout;

  InMemoryPriorityQueueService() {
    this.queues = new ConcurrentHashMap<>();

    String propFileName = "config.properties";
    Properties confInfo = new Properties();

    try (InputStream inStream = getClass().getClassLoader().getResourceAsStream(propFileName)) {
      confInfo.load(inStream);
    } catch (IOException e) {
      e.printStackTrace();
    }

    this.visibilityTimeout = Integer.parseInt(confInfo.getProperty("visibilityTimeout", "30"));
  }

  @Override
  public void push(String queueUrl, String msgBody) {
    Queue<Message> queue = queues.get(queueUrl);
    if (queue == null) {
      queue = new PriorityQueue<>(Comparator.comparingInt(Message::getPriority).reversed().thenComparingLong(Message::getTimestamp));
      queues.put(queueUrl, queue);
    }

    queue.add(new Message(msgBody, this.extractPriorityFromJson(msgBody)));
  }

  @Override
  public Message pull(String queueUrl) {
    Queue<Message> queue = queues.get(queueUrl);
    if (queue == null || queue.isEmpty()) {
      return null;
    }

    Message msg = queue.peek();
    msg.setReceiptId(UUID.randomUUID().toString());
    msg.incrementAttempts();
    msg.setVisibleFrom(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(visibilityTimeout));

    return new Message(msg.getBody(), msg.getReceiptId(), this.extractPriorityFromJson(msg.getBody()));
  }

  @Override
  public void delete(String queueUrl, String receiptId) {
    Queue<Message> queue = queues.get(queueUrl);

    if(queue == null || queue.isEmpty()) {
      return;
    }

    long nowTime = now();
    for (Iterator<Message> it = queue.iterator(); it.hasNext(); ) {
      Message msg = it.next();
      if (!msg.isVisibleAt(nowTime) && msg.getReceiptId().equals(receiptId)) {
        it.remove();
        break;
      }
    }
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

  long now() {
    return System.currentTimeMillis();
  }
}
