# Bridge-EX-Scheduler
Bridge EX 2.0 Scheduler

To test locally
mvn3 compile exec:java -Dexec.mainClass=org.sagebionetworks.bridge.exporter.scheduler.SchedulerLauncher \
-Dexec.args=[scheduler name]

The "compile" is important because otherwise exec:java may execute a stale version of your code
