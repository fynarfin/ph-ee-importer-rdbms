package hu.dpc.phee.operator.streams;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class JsonArraySerde implements Serde<JsonArray> {

    @Override
    public Serializer<JsonArray> serializer() {
        return new JsonArraySerializer();
    }

    @Override
    public Deserializer<JsonArray> deserializer() {
        return new JsonArrayDeserializer();
    }

    public static class JsonArraySerializer implements Serializer<JsonArray> {
        @Override
        public byte[] serialize(String topic, JsonArray data) {
            return data.toString().getBytes(StandardCharsets.UTF_8);
        }
    }

    public static class JsonArrayDeserializer implements Deserializer<JsonArray> {
        @Override
        public JsonArray deserialize(String topic, byte[] data) {
            return JsonParser.parseString(new String(data, StandardCharsets.UTF_8)).getAsJsonArray();
        }
    }
}
