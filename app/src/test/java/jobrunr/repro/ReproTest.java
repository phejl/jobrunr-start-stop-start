package jobrunr.repro;

import org.jobrunr.configuration.JobRunr;
import org.jobrunr.scheduling.JobScheduler;
import org.jobrunr.scheduling.RecurringJobBuilder;
import org.jobrunr.server.BackgroundJobServer;
import org.jobrunr.server.JobActivator;
import org.jobrunr.storage.nosql.mongo.MongoDBStorageProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Testcontainers
public class ReproTest {
        
    private static final Logger LOGGER = LoggerFactory.getLogger(ReproTest.class);
    
    @Container
    final MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:4.0.4"));
   
    @AfterEach
    public void tearDown() {
         JobRunr.getBackgroundJobServer().stop();
    }
        
    @Test
    void testStartStopStartPerformingMissedRecurringJob() throws InterruptedException {
        final AtomicInteger count = new AtomicInteger();
        
        final JobScheduler jobScheduler = JobRunr.configure()
                .useStorageProvider(new MongoDBStorageProvider(mongoDBContainer.getHost(), mongoDBContainer.getFirstMappedPort()))
                .useJobActivator(new JobActivator() {

                    @Override
                    public <T> T activateJob(Class<T> type) {
                        return type.cast(new TestJob(count));
                    }

                })
                .useBackgroundJobServer()
                .initialize()
                .getJobScheduler();
        final BackgroundJobServer server = JobRunr.getBackgroundJobServer();

        final TestJob testJob = new TestJob(count);
        jobScheduler.createRecurrently(
                RecurringJobBuilder.aRecurringJob()
                        .withId("repro")
                        .withDuration(Duration.of(15, ChronoUnit.SECONDS))
                        .withAmountOfRetries(0)
                        .withDetails(() -> testJob.perform()));
        Thread.sleep(30000);
        Assertions.assertTrue(count.get() > 0);

        LOGGER.info("Stopping the server");
        server.stop();
        int lastCount = count.get();
        LOGGER.info("So far the job has been executed {} times", count);
        Thread.sleep(60000);
        
        LOGGER.info("Starting the server");
        server.start();
        Thread.sleep(30000);
        
        int current = count.get();
        Assertions.assertTrue(current <= lastCount + 2, "The job has been scheduled again for the times the server was stopped; " +
                                                            "maximum expected " + (lastCount + 2) + " but was " + current);
        
    }
    
}
