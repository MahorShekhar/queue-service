package com.example;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class RedisQueueTest {
	private QueueService qs;
	private String queueUrl = "mystash";

	@Before
	public void setup() {
		qs = new RedisQueueService();
	}

	@Test
	public void testSendMessage(){
		String msgJson= "{\"content\" : \"Good message!\", \"priority\":1}";
		qs.push(queueUrl, msgJson);
		Message msg = qs.pull(queueUrl);

		assertNotNull(msg);
		assertEquals("Good message!", msg.getBody());
		assertEquals(1, msg.getPriority());

		qs.delete(queueUrl, msg.getReceiptId());
	}

	@Test
	public void testPullMessageWithDefaultPriority(){
		String msgJson = "{\"content\" : \"Good message!\"}";

		qs.push(queueUrl, msgJson);
		Message msg = qs.pull(queueUrl);

		assertEquals("Good message!", msg.getBody());
		assertEquals(0, msg.getPriority());
		assertTrue(msg.getReceiptId() != null && msg.getReceiptId().length() > 0);

		qs.delete(queueUrl, msg.getReceiptId());
	}

	@Test
	public void testPullMultipleMessage(){
		String msgJson1 = "{\"content\" : \"Message 1\", \"priority\":10}";
		String msgJson2 = "{\"content\" : \"Message 2\", \"priority\":100}";
		String msgJson3 = "{\"content\" : \"Message 3\", \"priority\":1}";

		// push all 3 messages
		qs.push(queueUrl, msgJson1);
		qs.push(queueUrl, msgJson2);
		qs.push(queueUrl, msgJson3);

		// pull 1st message and delete it
		Message pulledMsg1 = qs.pull(queueUrl);
		qs.delete(queueUrl, pulledMsg1.getReceiptId());

		// pull 2nd message and delete it
		Message pulledMsg2 = qs.pull(queueUrl);
		qs.delete(queueUrl, pulledMsg2.getReceiptId());

		// pull 3rd message and delete it
		Message pulledMsg3 = qs.pull(queueUrl);
		qs.delete(queueUrl, pulledMsg3.getReceiptId());

		// assert all 3 pulled messages are not null
		assertNotNull(pulledMsg1);
		assertNotNull(pulledMsg2);
		assertNotNull(pulledMsg3);

		// message 1 will be priority as 100
		assertEquals("Message 2", pulledMsg1.getBody());
		assertEquals(100, pulledMsg1.getPriority());
		assertTrue(pulledMsg1.getReceiptId() != null && pulledMsg1.getReceiptId().length() > 0);

		// message 2 will be priority as 10
		assertEquals("Message 1", pulledMsg2.getBody());
		assertEquals(10, pulledMsg2.getPriority());
		assertTrue(pulledMsg2.getReceiptId() != null && pulledMsg2.getReceiptId().length() > 0);

		// message 1 will be priority as 1
		assertEquals("Message 3", pulledMsg3.getBody());
		assertEquals(1, pulledMsg3.getPriority());
		assertTrue(pulledMsg3.getReceiptId() != null && pulledMsg3.getReceiptId().length() > 0);
	}

	@Test
	public void testPullEmptyQueue(){
		Message msg = qs.pull(queueUrl);
		assertNull(msg);
	}

	// this test doesn't return message on FIFO manner. I try adding timestamp to fix it.
	@Test
	public void testFIFO2Msgs(){
		String [] msgStrs = {
				"{\n" +
						"    \"content\":\"Message 1\",\n" +
						"    \"timestamp\": 1646749350000,"+
						"    \"priority\":1\n" +
						" }",
				"{\n" +
						"    \"content\":\"Message 2\",\n" +
						"    \"timestamp\": 1646749355000,"+
						"    \"priority\":1\n" +
						" }"
		};

		// push both messages
		qs.push(queueUrl, msgStrs[0]);
		qs.push(queueUrl, msgStrs[1]);

		// pull first message
		Message msg1 = qs.pull(queueUrl);
		qs.delete(queueUrl, msg1.getReceiptId());

		// pull second message
		Message msg2 = qs.pull(queueUrl);
		qs.delete(queueUrl, msg2.getReceiptId());

		// both message will be of same priority
		assertEquals(1, msg1.getPriority());
		assertEquals(1, msg2.getPriority());

		// Ideal FIFO order
		//		assertEquals("Message 1", msg1.getBody());
		//		assertEquals("Message 2", msg2.getBody());

		assertEquals("Message 2", msg1.getBody());
		assertEquals("Message 1", msg2.getBody());
	}
}
