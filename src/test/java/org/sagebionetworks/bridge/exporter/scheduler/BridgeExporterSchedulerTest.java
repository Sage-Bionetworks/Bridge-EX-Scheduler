package org.sagebionetworks.bridge.exporter.scheduler;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BridgeExporterSchedulerTest {
    private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();
    private static final String TEST_SCHEDULER_NAME = "test-scheduler";
    private static final String TEST_SQS_QUEUE_URL = "test-sqs-queue";
    private static final String TEST_TIME_ZONE = "Asia/Tokyo";

    private AmazonSQSClient mockSqsClient;
    private BridgeExporterScheduler scheduler;
    private Item schedulerConfig;

    @BeforeMethod
    public void before() {
        // mock now
        DateTime mockNow = DateTime.parse("2016-02-01T16:47+0900");
        DateTimeUtils.setCurrentMillisFixed(mockNow.getMillis());

        // mock DDB
        Table mockConfigTable = mock(Table.class);
        schedulerConfig = new Item().withString("sqsQueueUrl", TEST_SQS_QUEUE_URL)
                .withString("timeZone", TEST_TIME_ZONE);
        when(mockConfigTable.getItem("schedulerName", TEST_SCHEDULER_NAME)).thenReturn(schedulerConfig);

        // mock SQS
        mockSqsClient = mock(AmazonSQSClient.class);

        // set up scheduler
        scheduler = new BridgeExporterScheduler();
        scheduler.setDdbExporterConfigTable(mockConfigTable);
        scheduler.setSqsClient(mockSqsClient);
    }

    @AfterMethod
    public void after() {
        // reset now
        DateTimeUtils.setCurrentMillisSystem();
    }

    @Test
    public void normalCase() throws Exception {
        // execute
        scheduler.schedule(TEST_SCHEDULER_NAME);

        // validate
        ArgumentCaptor<String> sqsMessageCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockSqsClient).sendMessage(eq(TEST_SQS_QUEUE_URL), sqsMessageCaptor.capture());

        String sqsMessage = sqsMessageCaptor.getValue();
        JsonNode sqsMessageNode = JSON_OBJECT_MAPPER.readTree(sqsMessage);
        assertEquals(sqsMessageNode.size(), 2);
        assertEquals(sqsMessageNode.get("date").textValue(), "2016-01-31");
        assertEquals(sqsMessageNode.get("tag").textValue(), "[scheduler=" + TEST_SCHEDULER_NAME + ";date=2016-01-31]");
    }

    @Test
    public void withRequestOverride() throws Exception {
        // add request override
        schedulerConfig.withString("requestOverrideJson", "{\"foo\":\"bar\"}");

        // execute
        scheduler.schedule(TEST_SCHEDULER_NAME);

        // validate
        ArgumentCaptor<String> sqsMessageCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockSqsClient).sendMessage(eq(TEST_SQS_QUEUE_URL), sqsMessageCaptor.capture());

        String sqsMessage = sqsMessageCaptor.getValue();
        JsonNode sqsMessageNode = JSON_OBJECT_MAPPER.readTree(sqsMessage);
        assertEquals(sqsMessageNode.size(), 3);
        assertEquals(sqsMessageNode.get("date").textValue(), "2016-01-31");
        assertEquals(sqsMessageNode.get("tag").textValue(), "[scheduler=" + TEST_SCHEDULER_NAME + ";date=2016-01-31]");
        assertEquals(sqsMessageNode.get("foo").textValue(), "bar");
    }
}
