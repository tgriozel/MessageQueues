package namespace;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClient;
import namespace.service.SqsQueueService;
import org.junit.BeforeClass;

public class SqsQueueTest extends QueueTest {

    @BeforeClass
    public static void testInit() {
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        }
        catch (Exception e) {
            throw new AmazonClientException(
                "Cannot load the credentials from the credential profiles file. "
                + "Please make sure that your credentials file is at the correct "
                + "location (~/.aws/credentials), and is in valid format.", e);
        }
        service = new SqsQueueService(new AmazonSQSClient(credentials));
    }
}
