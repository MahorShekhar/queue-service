package com.example;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class InMemoryPriorityQueueTest {
	private QueueService qs;
	private String queueUrl = "https://sqs.ap-1.amazonaws.com/007/MyQueue";
	
	@Before
	public void setup() {
		qs = new InMemoryPriorityQueueService();
	}
	
	
	@Test
	public void testSendMessage(){
		String msgBody = "{ \"name\":\"John\", \"age\":30, \"car\":null }";
		qs.push(queueUrl, msgBody);
		Message msg = qs.pull(queueUrl);

		assertNotNull(msg);
		assertEquals(msgBody, msg.getBody());
	}
	
	@Test
	public void testPullMessageWithDefaultPriority(){
		String msgBody = "{ \"name\":\"John\", \"age\":30, \"car\":null }";
		
		qs.push(queueUrl, msgBody);
		Message msg = qs.pull(queueUrl);

		assertEquals(msgBody, msg.getBody());
		assertEquals(0, msg.getPriority());
		assertTrue(msg.getReceiptId() != null && msg.getReceiptId().length() > 0);
	}

	@Test
	public void testPullMessageWithDefinedPriority(){
		String msgBody = "{ \"name\":\"John\", \"age\":30, \"car\":null , \"priority\":100}";

		qs.push(queueUrl, msgBody);
		Message msg = qs.pull(queueUrl);

		assertEquals(msgBody, msg.getBody());
		assertEquals(100, msg.getPriority());
		assertTrue(msg.getReceiptId() != null && msg.getReceiptId().length() > 0);
	}

	@Test
	public void testPullMultipleMessage(){
		String msgBody1 = "{ \"name\":\"John\", \"age\":30, \"car\":null , \"priority\":10}";
		String msgBody2 = "{ \"name\":\"John\", \"age\":30, \"car\":null , \"priority\":100}";
		String msgBody3 = "{ \"name\":\"John\", \"age\":30, \"car\":null , \"priority\":1}";

		// push all 3 messages
		qs.push(queueUrl, msgBody1);
		qs.push(queueUrl, msgBody2);
		qs.push(queueUrl, msgBody3);

		 // pull 1st message and delete it
		Message pulledMsg1 = qs.pull(queueUrl);
		qs.delete(queueUrl , pulledMsg1.getReceiptId());

		// pull 2nd message and delete it
		Message pulledMsg2 = qs.pull(queueUrl);
		qs.delete(queueUrl , pulledMsg2.getReceiptId());

		// pull 3rd message and delete it
		Message pulledMsg3 = qs.pull(queueUrl);
		qs.delete(queueUrl , pulledMsg3.getReceiptId());

		// assert all 3 pulled messages are not null
		assertNotNull(pulledMsg1);
		assertNotNull(pulledMsg2);
		assertNotNull(pulledMsg3);

		// message 1 will be priority as 100
		assertEquals(msgBody2, pulledMsg1.getBody());
		assertEquals(100, pulledMsg1.getPriority());
		assertTrue(pulledMsg1.getReceiptId() != null && pulledMsg1.getReceiptId().length() > 0);

		// message 2 will be priority as 10
		assertEquals(msgBody1, pulledMsg2.getBody());
		assertEquals(10, pulledMsg2.getPriority());
		assertTrue(pulledMsg2.getReceiptId() != null && pulledMsg2.getReceiptId().length() > 0);

		// message 1 will be priority as 1
		assertEquals(msgBody3, pulledMsg3.getBody());
		assertEquals(1, pulledMsg3.getPriority());
		assertTrue(pulledMsg3.getReceiptId() != null && pulledMsg3.getReceiptId().length() > 0);
	}

	@Test
	public void testPullEmptyQueue(){
		Message msg = qs.pull(queueUrl);
		assertNull(msg);
	}
	
	@Test
	public void testDeleteMessage(){
		String msgBody = "{ \"name\":\"John\", \"age\":30, \"car\":null, \"priority\":1 }";
		
		qs.push(queueUrl, msgBody);
		Message msg = qs.pull(queueUrl);

		qs.delete(queueUrl, msg.getReceiptId());
		msg = qs.pull(queueUrl);
		
		assertNull(msg);
	}
	
	@Test
	public void testFIFO3Msgs(){
		String [] msgStrs = {
				"{\n" +
				"    \"name\":\"John1\",\n" +
				"    \"age\":30,\n" +
				"    \"priority\":1\n" +
				" }",
				"{\n" +
				"    \"name\":\"John2\",\n" +
				"    \"age\":30,\n" +
				"    \"priority\":1\n" +
				" }",
				"{\n" +
				"    \"name\":\"John3\",\n" +
				"    \"age\":30,\n" +
				"    \"priority\":1\n" +
				" }"
		};

		// push all 3 messages
		qs.push(queueUrl, msgStrs[0]);
		qs.push(queueUrl, msgStrs[1]);
		qs.push(queueUrl, msgStrs[2]);

		// pull first message
		Message msg1 = qs.pull(queueUrl);
		qs.delete(queueUrl , msg1.getReceiptId());

		// pull second message
		Message msg2 = qs.pull(queueUrl);
		qs.delete(queueUrl , msg2.getReceiptId());

		// pull third message
		Message msg3 = qs.pull(queueUrl);
		qs.delete(queueUrl , msg3.getReceiptId());

		// all messages will be of same priority
		assertEquals(1, msg1.getPriority());
		assertEquals(1, msg2.getPriority());
		assertEquals(1, msg3.getPriority());

		// assert first pulled message has msgBody1 and likewise for second & third message
		assertEquals(msgStrs[0],msg1.getBody());
		assertEquals(msgStrs[1],msg2.getBody());
		assertEquals(msgStrs[2],msg3.getBody());
	}

	@Test
	public void testAckTimeout(){

		String msgBody = "{ \"name\":\"John\", \"age\":30, \"car\":null, \"priority\":1 }";
		InMemoryPriorityQueueService queueService = new InMemoryPriorityQueueService() {
			long now() {
				return System.currentTimeMillis() + 1000 * 30 + 1;
			}
		};

		queueService.push(queueUrl, msgBody);
		queueService.pull(queueUrl);
		Message msg = queueService.pull(queueUrl);
		assertTrue(msg != null && msg.getBody() == msgBody);
	}
}
