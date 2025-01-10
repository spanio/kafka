/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.tools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.tools.api.RecordReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * A custom implementation of {@link RecordReader} that reads JSON objects containing base64-encoded values, and
 * optionally a string-encoded key.
 *
 * It could be easily extended to support headers, but I didn't need that at the time I wrote it.
 *
 * <p></p>
 * <pre>
 *    topic                : the topic to which the records should be sent.
 *    parse.key            : indicates if a record's key needs to be parsed. When false the key is always sent as null (default: false).
 *    key.property         : The name of the json property containing the key. (default: key).
 *    value.property       : The name of the json property containing the value. (default: value).
 *    ignore.error         : whether best attempts should be made to ignore parsing errors. (default: false).
 *    null.marker          : record key, record value, header key, header value which match this marker are replaced by null. (default: null).
 * </pre>
 */
public class JsonBase64LineMessageReader implements RecordReader {
    private String topic;
    private String valueProperty = "value";
    private boolean parseKey;
    private String keyProperty = "key";
    private boolean parseHeaders;
    private String headersProperty = "headers";
    private boolean ignoreError;
    private int lineNumber;
    private final boolean printPrompt = System.console() != null;
    private String nullMarker;

    @Override
    public void configure(Map<String, ?> props) {
        topic = props.get("topic").toString();
        if (props.containsKey("value.property"))
            valueProperty = props.get("value.property").toString();
        if (props.containsKey("parse.key"))
            parseKey = props.get("parse.key").toString().trim().equalsIgnoreCase("true");
        if (props.containsKey("key.property"))
            keyProperty = props.get("key.property").toString();
        if (props.containsKey("parse.headers"))
            parseHeaders = props.get("parse.headers").toString().trim().equalsIgnoreCase("true");
        if (props.containsKey("headers.property"))
            headersProperty = props.get("headers.property").toString();
        if (props.containsKey("ignore.error"))
            ignoreError = props.get("ignore.error").toString().trim().equalsIgnoreCase("true");
        if (props.containsKey("null.marker"))
            nullMarker = props.get("null.marker").toString();
        if (keyProperty.equals(nullMarker))
            throw new KafkaException("null.marker and key.property may not be equal");
        if (headersProperty.equals(nullMarker))
            throw new KafkaException("null.marker and headers.property may not be equal");
    }

    @Override
    public Iterator<ProducerRecord<byte[], byte[]>> readRecords(InputStream inputStream) {
        ObjectMapper mapper = new ObjectMapper();
        Base64.Decoder decoder = Base64.getDecoder();
        return new Iterator<ProducerRecord<byte[], byte[]>>() {
            private final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            private ProducerRecord<byte[], byte[]> current;
            private JsonNode json;

            @Override
            public boolean hasNext() {
                if (current != null) {
                    return true;
                } else {
                    lineNumber += 1;
                    if (printPrompt) {
                        System.out.print(">");
                    }

                    String line;
                    try {
                        line = reader.readLine();
                    } catch (IOException e) {
                        throw new KafkaException(e);
                    }

                    if (line == null) {
                        current = null;
                    } else {
                        try {
                            json = mapper.readTree(line);
                        } catch (JsonProcessingException e) {
                            throw new KafkaException(e);
                        }

                        String key = parseKey ? json.get(keyProperty).asText() : null;

                        String valueBase64 = json.get(valueProperty).asText();

                        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                                topic,
                                key != null && !key.equals(nullMarker) ? key.getBytes(StandardCharsets.UTF_8) : null,
                                valueBase64 != null && !valueBase64.equals(nullMarker) ? decoder.decode(valueBase64) : null
                        );

//                        if (headers != null && !headers.equals(nullMarker)) {
//                            stream(splitHeaders(headers)).forEach(header -> record.headers().add(header.key(), header.value()));
//                        }
                        current = record;
                    }

                    return current != null;
                }
            }

            @Override
            public ProducerRecord<byte[], byte[]> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("no more record");
                } else {
                    try {
                        return current;
                    } finally {
                        current = null;
                    }
                }
            }
        };
    }

    private String parse(boolean enabled, String line, int startIndex, String demarcation, String demarcationName) {
        if (!enabled) {
            return null;
        }
        int index = line.indexOf(demarcation, startIndex);
        if (index == -1) {
            if (ignoreError) {
                return null;
            }
            throw new KafkaException("No " + demarcationName + " found on line number " + lineNumber + ": '" + line + "'");
        }
        return line.substring(startIndex, index);
    }

//    private Header[] parseHeaders(JsonNode headers) {
//        return new Header[] {};
//        This needs some different syntax to work, but I don't care about headers right now
//        return stream(headers.fields())
//                .map(entry -> {
//                    String headerKey = entry.getKey();
//                    if (headerKey.equals(nullMarker)) {
//                        throw new KafkaException("Header keys should not be equal to the null marker '" + nullMarker + "' as they can't be null");
//                    }
//
//                    JsonNode value = entry.getValue();
//                    byte[] headerValue = value.isNull() ? null : value.asText().getBytes(StandardCharsets.UTF_8);
//                    return new RecordHeader(headerKey, headerValue);
//                }).toArray(Header[]::new);
//    }

    // Visible for testing
    String keySeparator() {
        return keyProperty;
    }

    // Visible for testing
    boolean parseKey() {
        return parseKey;
    }

    // Visible for testing
    boolean parseHeaders() {
        return parseHeaders;
    }
}
