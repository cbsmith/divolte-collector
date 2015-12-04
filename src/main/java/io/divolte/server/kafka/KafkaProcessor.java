/*
 * Copyright 2014 GoDataDriven B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.divolte.server.kafka;

import static io.divolte.server.processing.ItemProcessor.ProcessingDirective.*;
import io.divolte.server.AvroRecordBuffer;
import io.divolte.server.config.ValidatedConfiguration;
import io.divolte.server.processing.ItemProcessor;

import java.io.Closeable;
import java.util.Objects;
import java.util.Queue;
import java.util.Map;
import java.util.HashMap;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ParametersAreNonnullByDefault
public class KafkaProcessor implements ItemProcessor<AvroRecordBuffer>, Callback, Closeable {
    private final static Logger logger = LoggerFactory.getLogger(KafkaProcessor.class);

    private final String topic;
    private final Producer<AvroRecordBuffer, AvroRecordBuffer> producer;

    public KafkaProcessor(final ValidatedConfiguration vc) {
        Objects.requireNonNull(vc);
        topic = vc.configuration().kafkaFlusher.topic;
        final Serializer<AvroRecordBuffer> keySerializer = new AvroRecordSerializer();
        final Serializer<AvroRecordBuffer> valueSerializer = new AvroRecordSerializer();
        final Map<String, Object> conf = new HashMap<String, Object>();
        for (String property : vc.configuration().kafkaFlusher.producer.stringPropertyNames()) {
            conf.put(property, vc.configuration().kafkaFlusher.producer.getProperty(property));
        }
        keySerializer.configure(conf, true);
        valueSerializer.configure(conf, false);
        producer = new KafkaProducer<AvroRecordBuffer, AvroRecordBuffer>(vc.configuration().kafkaFlusher.producer, keySerializer, valueSerializer);
    }

    @Override
    public void close() {
        producer.close();
    }

    @Override
    public ProcessingDirective process(final AvroRecordBuffer record) {
        send(record);
        return CONTINUE;
    }

    public void send(final AvroRecordBuffer record) {
        try {
            logger.debug("Processing individual record: {}", record);
            final ProducerRecord<AvroRecordBuffer, AvroRecordBuffer> pr = new ProducerRecord<AvroRecordBuffer, AvroRecordBuffer>(topic, record, record);
            producer.send(pr, this);
            logger.debug("Sent individual record to Kafka: {}", pr);
        } catch (final RuntimeException e) {
            logger.error("Received error while sending record {}", record, e);
        }
    }

    @Override
    public ProcessingDirective process(final Queue<AvroRecordBuffer> batch) {
        final int batchSize = batch.size();
        switch (batchSize) {
        case 0:
            logger.warn("Ignoring empty batch of events.");
            break;
        case 1:
            send(batch.remove());
            break;
        default:
            logger.debug("Processing batch of {} records.", batchSize);
            try {
                batch.stream()
                    .forEach(this::send);
                logger.debug("Processed batch of {} records.", batchSize);
            } finally {
                batch.clear();
                logger.debug("Cleared batch of {} records.", batchSize);
            }
         }
        return CONTINUE;
    }

    @Override
    public ProcessingDirective heartbeat() {
        logger.debug("Received heartbeat");
        return CONTINUE;
    }

    @Override
    public void onCompletion(final RecordMetadata metadata, final Exception exception) {
        if (exception != null) {
            logger.error("Error sending record {}", metadata, exception);
        } else {
            logger.debug("Completed sending {}", metadata);
        }
    }
}
