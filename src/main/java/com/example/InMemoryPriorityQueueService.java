package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryPriorityQueueService implements QueueService {
  private final Map<String, Queue<Message>> queues;

  InMemoryPriorityQueueService() {
    this.queues = new ConcurrentHashMap<>();
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

    return new Message(msg.getBody(), msg.getReceiptId(), this.extractPriorityFromJson(msg.getBody()));
  }

  @Override
  public void delete(String queueUrl, String receiptId) {
    Queue<Message> queue = queues.get(queueUrl);

    if(queue == null || queue.isEmpty()) {
      return;
    }

    for (Iterator<Message> it = queue.iterator(); it.hasNext(); ) {
      Message msg = it.next();
      if (msg.getReceiptId().equals(receiptId)) {
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
}
