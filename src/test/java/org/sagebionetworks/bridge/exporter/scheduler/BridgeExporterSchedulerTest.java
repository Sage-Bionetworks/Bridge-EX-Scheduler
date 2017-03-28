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

    private static final DateTime MOCK_NOW = DateTime.parse("2016-02-01T16:47:55.273+0900");

    private static final DateTime DAILY_END_DATE_TIME = MOCK_NOW
            .withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0).minusMillis(1);

    private AmazonSQSClient mockSqsClient;
    private BridgeExporterScheduler scheduler;
    private Item schedulerConfig;

    @BeforeMethod
    public void before() {
        // mock now
        DateTimeUtils.setCurrentMillisFixed(MOCK_NOW.getMillis());

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
    public void defaultScheduleType() throws Exception {
        // No scheduleType in DDB config. No setup needed. Just call test helper directly.
        dailyScheduleHelper();
    }

    @Test
    public void dailyScheduleType() throws Exception {
        schedulerConfig.withString("scheduleType", "DAILY");
        dailyScheduleHelper();
    }

    @Test
    public void invalidScheduleType() throws Exception {
        // falls back to DAILY
        schedulerConfig.withString("scheduleType", "INVALID_TYPE");
        dailyScheduleHelper();
    }

    // Helper method for all daily schedule test cases.
    private void dailyScheduleHelper() throws Exception {
        // execute
        scheduler.schedule(TEST_SCHEDULER_NAME);

        // validate
        ArgumentCaptor<String> sqsMessageCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockSqsClient).sendMessage(eq(TEST_SQS_QUEUE_URL), sqsMessageCaptor.capture());

        String sqsMessage = sqsMessageCaptor.getValue();
        JsonNode sqsMessageNode = JSON_OBJECT_MAPPER.readTree(sqsMessage);
        assertEquals(sqsMessageNode.size(), 3);
        assertEquals(sqsMessageNode.get("exportType").textValue(), "DAILY");
        assertEquals(sqsMessageNode.get("endDateTime").textValue(), DAILY_END_DATE_TIME.toString());
        assertEquals(sqsMessageNode.get("tag").textValue(), "[scheduler=" + TEST_SCHEDULER_NAME + ";endDateTime=" + DAILY_END_DATE_TIME.toString() + "]");
    }

    @Test
    public void hourlyScheduleType() throws Exception {
        // Add hourly schedule type param. For realism, also add study whitelist.
        schedulerConfig.withString("scheduleType", "HOURLY").withString("requestOverrideJson",
                "{\"studyWhitelist\":[\"hourly-study\"]}");

        // execute
        scheduler.schedule(TEST_SCHEDULER_NAME);

        // validate
        ArgumentCaptor<String> sqsMessageCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockSqsClient).sendMessage(eq(TEST_SQS_QUEUE_URL), sqsMessageCaptor.capture());

        String sqsMessage = sqsMessageCaptor.getValue();
        JsonNode sqsMessageNode = JSON_OBJECT_MAPPER.readTree(sqsMessage);
        assertEquals(sqsMessageNode.size(), 4);

        String endDateTimeStr = sqsMessageNode.get("endDateTime").textValue();
        assertEquals(DateTime.parse(endDateTimeStr), DateTime.parse("2016-02-01T16:00:00.000+0900"));

        JsonNode studyWhitelistNode = sqsMessageNode.get("studyWhitelist");
        assertEquals(studyWhitelistNode.size(), 1);
        assertEquals(studyWhitelistNode.get(0).textValue(), "hourly-study");

        assertEquals(sqsMessageNode.get("tag").textValue(), "[scheduler=" + TEST_SCHEDULER_NAME
                + ";endDateTime=" + endDateTimeStr + "]");
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
        assertEquals(sqsMessageNode.size(), 4);
        assertEquals(sqsMessageNode.get("exportType").textValue(), "DAILY");
        assertEquals(sqsMessageNode.get("endDateTime").textValue(), DAILY_END_DATE_TIME.toString());
        assertEquals(sqsMessageNode.get("tag").textValue(), "[scheduler=" + TEST_SCHEDULER_NAME + ";endDateTime=" + DAILY_END_DATE_TIME.toString() + "]");
        assertEquals(sqsMessageNode.get("foo").textValue(), "bar");
    }
}
