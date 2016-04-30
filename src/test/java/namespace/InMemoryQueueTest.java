package namespace;

import namespace.service.InMemoryQueueService;
import org.junit.BeforeClass;

public class InMemoryQueueTest extends QueueTest {

    @BeforeClass
    public static void testInit() {
        service = new InMemoryQueueService();
    }
}
