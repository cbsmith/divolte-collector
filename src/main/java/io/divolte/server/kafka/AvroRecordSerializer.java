package io.divolte.server.kafka;

import java.util.Map;
import java.util.function.BiFunction;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.errors.SerializationException;

import io.divolte.server.AvroRecordBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//Because AvroRecordBuffer really already handles serialization.
//This just handles the little bits that are left with Kafka's
//API's.

public class AvroRecordSerializer implements Serializer<AvroRecordBuffer> {
    public static final Logger logger = LoggerFactory.getLogger(AvroRecordSerializer.class);

    private BiFunction<String, AvroRecordBuffer, byte[]> serializer;

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // Front load figuring out if we are a key (therefore just grabbing the partyid field)
        // or a value (therefore grabbing the record's byte buffer
        if (isKey) {
            logger.debug("Creating key Serializer");
            //Mimic string encoder, but be more efficient
            Charset charset = StandardCharsets.UTF_8;
            Object encodingValue = configs.get("key.serializer.encoding");
            if (encodingValue == null) {
                encodingValue = configs.get("serializer.encoding");
            }
            if (encodingValue != null && encodingValue instanceof String) {
                final String encoding = (String) encodingValue;
                try {
                    charset = Charset.forName(encoding);
                } catch (final UnsupportedCharsetException e) {
                    logger.error("Could not find charset for {}", encoding, e);
                    throw new RuntimeException("Unsupported charset", e);
                }
            }

            logger.info("Serializer charset is {}", charset);

            final Charset selectedCharset = charset;
            serializer = (final String topic, final AvroRecordBuffer data) -> data.getPartyId().value.getBytes(selectedCharset);
        } else {
            logger.debug("Creating value Serializer");
            serializer = (final String topic, final AvroRecordBuffer data) -> {
                final ByteBuffer avroBuffer = data.getByteBuffer();
                final byte[] avroBytes = new byte[avroBuffer.remaining()];
                avroBuffer.get(avroBytes);
                return avroBytes;
            };
        }
    }

    @Override
    public byte[] serialize(String topic, AvroRecordBuffer data) {
        if (data == null) {
            return null;
        }

        try {
            return serializer.apply(topic, data);
        } catch (final RuntimeException e) {
            logger.error("Error while serializing AvroRecordBuffer {}", data, e);
            throw new SerializationException("Error serializing AvroRecord", e);
        }
    }

    @Override
    public void close() {
        //nuttin
    }
}
