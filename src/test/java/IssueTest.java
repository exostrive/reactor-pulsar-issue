import java.time.Duration;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import reactor.core.publisher.Mono;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.MessageSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumer;
import org.apache.pulsar.reactive.client.api.ReactiveMessagePipeline;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Publisher;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.assertThat;

public class IssueTest {

    static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("apachepulsar/pulsar");

    private static PulsarContainer container;

    @BeforeAll
    public static void beforeAll() {
        container = new PulsarContainer(DEFAULT_IMAGE_NAME.withTag("4.0.4"))
            .withEnv("PULSAR_PREFIX_brokerDeduplicationEnabled", "true")
            .withEnv("PULSAR_PREFIX_allowAutoTopicCreationType", "partitioned")
            .withEnv("PULSAR_PREFIX_defaultNumPartitions", "3");
        container.start();

        try(PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(container.getHttpServiceUrl()).build()) {
            admin.namespaces().createNamespace("public/test");
        } catch (PulsarClientException | PulsarAdminException e) {
            throw new RuntimeException(e);
        }
    }

    public PulsarClient pulsarClient() throws PulsarClientException {
        return PulsarClient.builder()
            .serviceUrl(container.getPulsarBrokerUrl())
            .enableTcpNoDelay(false)
            .build();
    }

    <T> ReactiveMessageConsumer<T> buildReactiveConsumer(ReactivePulsarClient reactivePulsarClient, Schema<T> schema, String topicName) {
        return reactivePulsarClient.messageConsumer(schema)
            .topic(topicName)
            .subscriptionName("sub")
            .subscriptionMode(SubscriptionMode.Durable)
            .subscriptionType(SubscriptionType.Shared)
            .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
            .build();
    }

    @ParameterizedTest()
    @ValueSource(ints = {0, 1, 2 })
    public void testStartStopStartPipeline(int restarts) throws PulsarClientException {
        final String topicName = "persistent://public/test/topic";

        try (PulsarClient client = pulsarClient()) {
            ReactivePulsarClient reactivePulsarClient = AdaptedReactivePulsarClientFactory.create(client);
            ReactiveMessageSender<Integer> reactiveMessageSender = reactivePulsarClient
                .messageSender(Schema.INT32)
                .topic(topicName)
                .chunkingEnabled(false)
                .batchingEnabled(false)
                .build();

            for(int i = 0; i < restarts; i++) {
                final CountDownLatch count = new CountDownLatch(1);
                Function<Message<Integer>, Publisher<Void>> handler = message -> {
                    System.out.println("Message date=" + new Date() + " value=" + message.getValue());
                    count.countDown();
                    return Mono.empty();
                };
                try (ReactiveMessagePipeline pipeline = buildReactiveConsumer(reactivePulsarClient, Schema.INT32, topicName)
                        .messagePipeline()
                        .messageHandler(handler).build()) {
                    pipeline.start();
                    if (i == (restarts - 1)) {
                        reactiveMessageSender.sendOne(MessageSpec.of(restarts)).block();
                        assertThat(count.await(5, TimeUnit.SECONDS)).isTrue();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
