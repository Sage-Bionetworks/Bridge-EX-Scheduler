package org.sagebionetworks.bridge.exporter.scheduler;

import java.io.IOException;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;

/** Bridge-EX 2.0 Scheduler */
public class BridgeExporterScheduler {
    private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();

    private Table ddbExporterConfigTable;
    private AmazonSQSClient sqsClient;

    /**
     * DDB table for Bridge Exporter Scheduler configs. We store configs in DDB instead of in env vars or in a config
     * file because (1) DDB is easier to update for fast config changes and (2) AWS Lambda is a lightweight
     * infrastructure without env vars or config files.
     */
    public final void setDdbExporterConfigTable(Table ddbExporterConfigTable) {
        this.ddbExporterConfigTable = ddbExporterConfigTable;
    }

    /** SQS client, used to send the request to Bridge-EX. */
    public final void setSqsClient(AmazonSQSClient sqsClient) {
        this.sqsClient = sqsClient;
    }

    /**
     * Sends the Bridge-EX request for the given scheduler. This is called by AWS Lambda at the configured interval.
     * NOTE: This only calls Bridge-EX once. The actually scheduling logic is handled by AWS Lambda.
     *
     * @param schedulerName
     *         scheduler name, used to get scheduler configs
     * @throws IOException
     *         if constructing the Bridge-EX request fails
     */
    public void schedule(String schedulerName) throws IOException {
        // get scheduler config from DDB
        Item schedulerConfig = ddbExporterConfigTable.getItem("schedulerName", schedulerName);
        Preconditions.checkNotNull(schedulerConfig, "No configuration for scheduler " + schedulerName);

        String sqsQueueUrl = schedulerConfig.getString("sqsQueueUrl");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(sqsQueueUrl), "sqsQueueUrl not configured for scheduler "
                + schedulerName);

        String timeZoneStr = schedulerConfig.getString("timeZone");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(timeZoneStr), "timeZone not configured for scheduler "
                + schedulerName);
        DateTimeZone timeZone = DateTimeZone.forID(timeZoneStr);

        // make Bridge-EX request JSON
        ObjectNode requestNode;
        String requestOverrideJson = schedulerConfig.getString("requestOverrideJson");
        if (Strings.isNullOrEmpty(requestOverrideJson)) {
            requestNode = JSON_OBJECT_MAPPER.createObjectNode();
        } else {
            requestNode = (ObjectNode) JSON_OBJECT_MAPPER.readTree(requestOverrideJson);
        }

        // Parse schedule type
        ScheduleType scheduleType;
        String scheduleTypeStr = schedulerConfig.getString("scheduleType");
        if (Strings.isNullOrEmpty(scheduleTypeStr)) {
            // Defaults to DAILY
            scheduleType = ScheduleType.DAILY;
        } else {
            try {
                scheduleType = ScheduleType.valueOf(scheduleTypeStr);
            } catch (IllegalArgumentException ex) {
                // log error, default to DAILY
                System.err.println("Invalid schedule type: " + scheduleTypeStr);
                scheduleType = ScheduleType.DAILY;
            }
        }

        // insert date and tag
        switch (scheduleType) {
            case DAILY: {
                // Each day, we export the previous day's data.
                String yesterdaysDateString = LocalDate.now(timeZone).minusDays(1).toString();
                String tag = "[scheduler=" + schedulerName + ";date=" + yesterdaysDateString + "]";
                requestNode.put("date", yesterdaysDateString);
                requestNode.put("tag", tag);
            }
            break;
            case HOURLY: {
                // endDateTime is the start of the current hour. startDateTime is the hour before.
                DateTime endDateTime = DateTime.now(timeZone).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);
                String endDateTimeStr = endDateTime.toString();
                requestNode.put("endDateTime", endDateTimeStr);

                DateTime startDateTime = endDateTime.minusHours(1);
                String startDateTimeStr = startDateTime.toString();
                requestNode.put("startDateTime", startDateTimeStr);

                String tag = "[scheduler=" + schedulerName + ";startDateTime=" + startDateTimeStr + ";endDateTime=" +
                        endDateTimeStr + "]";
                requestNode.put("tag", tag);
            }
            break;
            default:
                throw new IllegalArgumentException("Impossible code path, scheduleType=" + scheduleType.name());
        }

        // write request to SQS
        String requestJson = JSON_OBJECT_MAPPER.writeValueAsString(requestNode);
        System.out.println("Sending request: sqsQueueUrl=" + sqsQueueUrl + ", requestJson=" + requestJson);
        sqsClient.sendMessage(sqsQueueUrl, requestJson);
    }

    /** Enum to distinguish between daily and hourly exports. */
    public enum ScheduleType {
        DAILY,
        HOURLY,
    }
}
