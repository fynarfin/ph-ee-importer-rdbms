package hu.dpc.phee.operator.streams;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.jayway.jsonpath.DocumentContext;
import hu.dpc.phee.operator.config.AnalyticsConfig;
import hu.dpc.phee.operator.config.TransferTransformerConfig;
import hu.dpc.phee.operator.entity.analytics.EventTimestampsRepository;
import hu.dpc.phee.operator.entity.analytics.EventTimestamps;
import hu.dpc.phee.operator.entity.batch.BatchRepository;
import hu.dpc.phee.operator.entity.outboundmessages.OutboundMessagesRepository;
import hu.dpc.phee.operator.entity.task.Task;
import hu.dpc.phee.operator.entity.task.TaskRepository;
import hu.dpc.phee.operator.entity.tenant.ThreadLocalContextUtil;
import hu.dpc.phee.operator.entity.transactionrequest.TransactionRequestRepository;
import hu.dpc.phee.operator.entity.transfer.TransferRepository;
import hu.dpc.phee.operator.entity.variable.Variable;
import hu.dpc.phee.operator.entity.variable.VariableRepository;
import hu.dpc.phee.operator.importer.JsonPathReader;
import hu.dpc.phee.operator.tenants.TenantsService;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class StreamsSetup {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Value("${importer.kafka.topic}")
    private String kafkaTopic;

    @Value("${importer.kafka.aggreation-window-seconds}")
    private int aggregationWindowSeconds;

    @Autowired
    private StreamsBuilder streamsBuilder;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    TransferTransformerConfig transferTransformerConfig;

    @Autowired
    TransferRepository transferRepository;

    @Autowired
    TransactionRequestRepository transactionRequestRepository;

    @Autowired
    BatchRepository batchRepository;

    @Autowired
    OutboundMessagesRepository outboundMessagesRepository;

    @Autowired
    TenantsService tenantsService;

    @Autowired
    RecordParser recordParser;

    @Autowired
    VariableRepository variableRepository;

    @Autowired
    TaskRepository taskRepository;

    @Autowired
    private EventTimestampsRepository eventTimestampsRepository;

    @Autowired AnalyticsConfig analyticsConfig;


    @PostConstruct
    public void setup() {
        logger.debug("## setting up kafka streams on topic `{}`, aggregating every {} seconds", kafkaTopic, aggregationWindowSeconds);

        streamsBuilder.stream(kafkaTopic, Consumed.with(STRING_SERDE, STRING_SERDE))
                .filter((key, value) -> !shouldFilterOut(value))
                .groupBy((key, value) -> extractCompositeKey(value))
                .windowedBy(TimeWindows.of(Duration.ofMillis(300)).grace(Duration.ofMillis(100)))
                .aggregate(
                    JsonArray::new,
                    (key, value, aggregate) -> {
                        aggregate.add(JsonParser.parseString(value));
                        return aggregate;
                    },
                    Materialized.with(Serdes.String(), new JsonArraySerde())
                )
                .toStream()
                .foreach((windowedKey, batch) -> {
                    String compositeKey = windowedKey.key();
                    process(compositeKey, batch);
                });

        // TODO kafka-ba kell leirni a vegen az entitaslistat, nem DB-be, hogy konzisztens es ujrajatszhato legyen !!
    }

    public void process(Object _key, Object _value) {
        String key = (String) _key;
        JsonArray records = (JsonArray) _value;

        if (records == null || records.size() == 0) {
            logger.warn("skipping processing, null records for key: {}", key);
            return;
        }

        logger.debug(key);
        logger.debug(String.valueOf(records.size()));

        String bpmn;
        String tenantName;
        String first = String.valueOf(records.get(0));

        DocumentContext sample = JsonPathReader.parse(first);
        try {
            Pair<String, String> bpmnAndTenant = retrieveTenant(sample);
            bpmn = bpmnAndTenant.getFirst();
            tenantName = bpmnAndTenant.getSecond();
            logger.trace("resolving tenant server connection for tenant: {}", tenantName);
            DataSource tenant = tenantsService.getTenantDataSource(tenantName);
            ThreadLocalContextUtil.setTenant(tenant);
            ThreadLocalContextUtil.setTenantName(tenantName);
        } catch (Exception e) {
            logger.error("failed to process first record: {}, skipping whole batch", first, e);
            return;
        }

        try {
            if (transferTransformerConfig.findFlow(bpmn).isEmpty()) {
                logger.warn("skip saving flow information, no configured flow found for bpmn: {}", bpmn);
                return;
            }
            Optional<TransferTransformerConfig.Flow> config = transferTransformerConfig.findFlow(bpmn);
            String flowType = getTypeForFlow(config);

            logger.debug("processing key: {}, records: {}", key, records);
            Long workflowInstanceKey = sample.read("$.value.processInstanceKey");
            String valueType = sample.read("$.valueType", String.class);
            logger.debug("processing {} events", valueType);

            transactionTemplate.executeWithoutResult(status -> {
                for (JsonElement record : records) {
                    try {
                        DocumentContext recordDocument = JsonPathReader.parse(String.valueOf(record));
                        if (analyticsConfig.enableEventsTimestampsDump.equals("true")) {
                            logToTimestampsTable(recordDocument);
                        }
                        logger.debug("from kafka: {}", recordDocument.jsonString());

                        Long workflowKey = recordDocument.read("$.value.processDefinitionKey");
                        Long timestamp = recordDocument.read("$.timestamp");
                        String bpmnElementType = recordDocument.read("$.value.bpmnElementType");
                        String elementId = recordDocument.read("$.value.elementId");
                        logger.debug("Processing document of type {}", valueType);

                        List<Object> entities = switch (valueType) {
                            case "DEPLOYMENT", "VARIABLE_DOCUMENT", "WORKFLOW_INSTANCE" -> List.of();
                            case "PROCESS_INSTANCE" -> {
                                yield recordParser.processWorkflowInstance(recordDocument, bpmn, workflowInstanceKey, timestamp, bpmnElementType, elementId, flowType, sample);
                            }

                            case "JOB" -> {
                                yield recordParser.processTask(recordDocument, workflowInstanceKey, valueType, workflowKey, timestamp);
                            }

                            case "VARIABLE" -> {
                                  yield recordParser.processVariables(records, bpmn, workflowInstanceKey, workflowKey, flowType, sample);
                            }

                            case "INCIDENT" -> {
                                yield recordParser.processIncident(timestamp, flowType, bpmn, sample, workflowInstanceKey);
                            }

                            default -> throw new IllegalStateException("Unexpected event type: " + valueType);
                        };
                        if (entities.size() != 0) {
                            logger.debug("Saving {} entities", entities.size());
                            entities.forEach(entity -> {
                                if (entity instanceof Variable) {
                                    variableRepository.save((Variable) entity);
                                } else if (entity instanceof Task) {
                                    taskRepository.save((Task) entity);
                                } else {
                                    throw new IllegalStateException("Unexpected entity type: " + entity.getClass());
                                }
                            });
                        }

                        if (valueType.equals("VARIABLE")) {
                            break;
                        }
                    } catch (Exception e) {
                        logger.error("failed to parse record: {}", record, e);
                    }
                }
            });
        } catch (Exception e) {
            logger.error("failed to process batch", e);

        } finally {
            ThreadLocalContextUtil.clear();
        }
    }

    private String getTypeForFlow(Optional<TransferTransformerConfig.Flow> config) {
        return config.map(TransferTransformerConfig.Flow::getType).orElse(null);
    }

    public Pair<String, String> retrieveTenant(DocumentContext record) {
        String bpmnProcessIdWithTenant = findBpmnProcessId(record);

        String[] split = bpmnProcessIdWithTenant.split("-");
        if (split.length < 2) {
            throw new RuntimeException("Invalid bpmnProcessId, has no tenant information: '" + bpmnProcessIdWithTenant + "'");
        }
        return Pair.of(split[0], split[1]);
    }

    private String findBpmnProcessId(DocumentContext record) {
        String bpmnProcessIdWithTenant = record.read("$.value.bpmnProcessId", String.class);
        if (bpmnProcessIdWithTenant == null) {
            logger.warn("can't find bpmnProcessId in record: {}, trying alternative ways..", record.jsonString());
            List<String> ids = record.read("$.value..bpmnProcessId", List.class);
            if (ids.size() > 1) {
                throw new RuntimeException("Invalid bpmnProcessIdWithTenant, has more than one bpmnProcessIds: '" + ids + "'");
            }
            bpmnProcessIdWithTenant = ids.get(0);
        }
        logger.debug("resolved bpmnProcessIdWithTenant: {}", bpmnProcessIdWithTenant);
        return bpmnProcessIdWithTenant;
    }

    public void logToTimestampsTable(DocumentContext incomingRecord) {
        try{
            EventTimestamps eventTimestamps = new EventTimestamps();
            eventTimestamps.setWorkflowInstanceKey(incomingRecord.read("$.value.processInstanceKey"));
            eventTimestamps.setExportedTime(incomingRecord.read("$.exportedTime"));
            eventTimestamps.setImportedTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ").format(new Date()));
            eventTimestamps.setZeebeTime(incomingRecord.read("$.timestamp").toString());
            eventTimestampsRepository.save(eventTimestamps);
        }catch (Exception e) {
            logger.debug(e.getMessage().toString() + " Error parsing record");
        }
    }

    private String extractCompositeKey(String value) {
        DocumentContext documentContext = JsonPathReader.parse(value);
        String workflowInstanceKey = documentContext.read("value.processInstanceKey").toString();
        String recordType = documentContext.read("valueType").toString();
        return workflowInstanceKey + "|" + recordType;
    }

    private boolean shouldFilterOut(String value) {
        DocumentContext documentContext = JsonPathReader.parse(value);
        String valueType = documentContext.read("valueType").toString();
        String intent = documentContext.read("$.intent", String.class);

        // Add the condition to filter out specific value types
        return "PROCESS_INSTANCE".equals(valueType) && !("START_EVENT".equals(intent) || "END_EVENT".equals(intent)); // Replace "specificValueType" with the actual value type you want to filter out
    }
}
