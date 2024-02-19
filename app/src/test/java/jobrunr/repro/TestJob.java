package jobrunr.repro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class TestJob {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(TestJob.class);
    
    private final AtomicInteger count;

    public TestJob(AtomicInteger count) {
        this.count = count;
    }

    public void perform() {
        LOGGER.info("Performed {}", count.incrementAndGet());
    }

}