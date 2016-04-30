package namespace;

import namespace.service.FileQueueService;
import org.junit.BeforeClass;

public class FileQueueTest extends QueueTest {

    @BeforeClass
    public static void testInit() {
        service = new FileQueueService();
    }
}
