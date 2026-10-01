package lab;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public final class LogProducer {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String TOPIC = "prac-lab-logs";
    private static final String[] SERVICES = {"checkout-api", "catalog-api", "payment-api"};

    // Kafka 4.3의 주요 기본값과 실습용 재정의 값을 명시합니다.
    static Properties producerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS",
                        "localhost:29092,localhost:39092,localhost:49092"));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "prac-lab-java-producer");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");

        // 객체를 JSON 문자열로 만든 뒤 UTF-8 바이트로 직렬화합니다.
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // false: key가 있으면 해시로 선택. 기본 partitioner 구현을 사용합니다.
        props.put(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG, false);
        props.put(ProducerConfig.PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG, true);
        props.put(ProducerConfig.PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG, 0);

        // 전송 보장: ISR 응답을 기다리고 같은 producer 세션의 재시도 중복을 방지합니다.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // batch.size는 파티션별 목표 바이트 크기이며 채워질 때까지 무조건 기다리지 않습니다.
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16_384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0); // 기본 5ms 대신 기존 즉시 전송 유지
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33_554_432L);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1_048_576);

        // 재시도 횟수보다 전체 전달 시간으로 실패를 제한합니다.
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000); // 기본 120초 → 실습 30초 : 최초 전송부터 retry까지 포함한 전체 전송 제한 시간
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000); // 기본 30초 → 10초 : 개별 요청 응답 대기 시간
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30_000L); // 기본 60초 → 30초
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100L);
        props.put(ProducerConfig.RETRY_BACKOFF_MAX_MS_CONFIG, 1_000L);
        props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 50L);
        props.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1_000L);
        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 300_000L);
        props.put(ProducerConfig.METADATA_MAX_IDLE_CONFIG, 300_000L);
        props.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 540_000L);
        props.put(ProducerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, 10_000L);
        props.put(ProducerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, 30_000L);
        props.put(ProducerConfig.SEND_BUFFER_CONFIG, 131_072);
        props.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, 32_768);
        return props;
    }

    public static void main(String[] args) {
        try {
            run(args);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            System.err.println("Producer interrupted");
            System.exit(1);
        } catch (Exception error) {
            System.err.println("Kafka producer failed; see results/producer-stderr.log");
            error.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static void run(String[] args) throws Exception {
        int count = 300;
        double rate = 5;
        String runId = null;
        for (int index = 0; index < args.length; index++) {
            String option = args[index];
            if (option.equals("--help") || option.equals("-h")) {
                System.out.println("Usage: produce [--count 300] [--rate 5] [--run-id ID]; rate=0 sends a burst");
                return;
            }
            if (!Set.of("--count", "--rate", "--run-id").contains(option) || ++index == args.length) {
                throw new IllegalArgumentException("Unknown option or missing value: " + option);
            }
            switch (option) {
                case "--count" -> count = Integer.parseInt(args[index]);
                case "--rate" -> rate = Double.parseDouble(args[index]);
                case "--run-id" -> runId = args[index];
                default -> throw new IllegalArgumentException(option);
            }
        }
        if (count <= 0 || !Double.isFinite(rate) || rate < 0) {
            throw new IllegalArgumentException("count must be positive and rate must be finite and nonnegative");
        }
        if (runId == null) {
            runId = Instant.now().truncatedTo(ChronoUnit.SECONDS).toString()
                    .replace("-", "").replace(":", "") + "-" + UUID.randomUUID().toString().substring(0, 8);
        }
        Path results = Path.of("results");
        Files.createDirectories(results);
        Map<String, Set<Integer>> keyPartitions = new LinkedHashMap<>();
        System.out.printf("run_id=%s; sending %d events at %s/s%n", runId, count, rate);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties());
             BufferedWriter raw = Files.newBufferedWriter(results.resolve("raw-events.ndjson"), StandardCharsets.UTF_8)) {
            for (int i = 1; i <= count; i++) {
                int status = i % 10 == 0 ? 500 : (i % 10 == 1 ? 404 : 200);
                String level = status == 500 ? "ERROR" : (status == 404 ? "WARN" : "INFO");
                String key = SERVICES[i % SERVICES.length];
                Map<String, Object> event = new LinkedHashMap<>();
                event.put("timestamp", Instant.now().truncatedTo(ChronoUnit.MILLIS).toString());
                event.put("event_id", runId + "-" + i);
                event.put("run_id", runId);
                event.put("sequence", i);
                event.put("service", key);
                event.put("level", " " + level + " ");
                event.put("message", "  demo request " + i + ": status=" + status + "  ");
                event.put("status", Integer.toString(status));
                event.put("duration_ms", Long.toString(20 + (i * 37L) % 480));
                String value = JSON.writeValueAsString(event);
                raw.write(value);
                raw.newLine();
                // 1건마다 응답을 기다려 기존 동기 전송을 유지합니다.
                // 이 방식에서는 여러 메시지가 한 배치에 모이는 효과가 제한됩니다.
                RecordMetadata metadata = producer.send(new ProducerRecord<>(TOPIC, key, value)) // 자동 retry
                        .get(30, TimeUnit.SECONDS);
                keyPartitions.computeIfAbsent(key, ignored -> new TreeSet<>()).add(metadata.partition());
                if (rate > 0) {
                    TimeUnit.NANOSECONDS.sleep((long) (1_000_000_000d / rate));
                }
            }
            producer.flush();
        }
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("run_id", runId);
        summary.put("sent", count);
        summary.put("rate", rate);
        summary.put("client_id", "prac-lab-java-producer");
        summary.put("key_partitions", keyPartitions);
        summary.put("finished_at", Instant.now().toString());
        Files.writeString(results.resolve("last-run.json"),
                JSON.writerWithDefaultPrettyPrinter().writeValueAsString(summary) + "\n", StandardCharsets.UTF_8);
        System.out.println(JSON.writeValueAsString(summary));
    }
}
