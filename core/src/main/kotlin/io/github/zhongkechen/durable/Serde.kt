package io.github.zhongkechen.durable

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule

/**
 * Converts values to and from the checkpoint payload representation.
 */
public interface Serde {
    public fun encode(value: Any?): String

    public fun <T> decode(
        payload: String,
        type: TypeRef<T>,
    ): T
}

/**
 * JSON serializer used by the clean-room runtime.
 */
public class JsonSerde(
    private val mapper: ObjectMapper = defaultMapper(),
) : Serde {
    override fun encode(value: Any?): String = mapper.writeValueAsString(value)

    override fun <T> decode(
        payload: String,
        type: TypeRef<T>,
    ): T = mapper.readValue(payload, mapper.typeFactory.constructType(type.type))

    public companion object {
        public fun defaultMapper(): ObjectMapper =
            ObjectMapper()
                .registerModule(
                    KotlinModule.Builder()
                        .build(),
                ).registerModule(JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    }
}
