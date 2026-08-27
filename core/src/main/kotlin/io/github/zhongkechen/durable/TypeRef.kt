package io.github.zhongkechen.durable

import com.fasterxml.jackson.core.type.TypeReference
import java.lang.reflect.Type

/**
 * Retains a generic JVM type at runtime without requiring callers to pass raw
 * Java classes.
 */
public class TypeRef<T> @PublishedApi internal constructor(
    @PublishedApi internal val jacksonType: TypeReference<T>,
) {
    public val type: Type
        get() = jacksonType.type

    public companion object {
        public inline fun <reified T> of(): TypeRef<T> =
            TypeRef(
                object : TypeReference<T>() {},
            )
    }
}

public inline fun <reified T> typeRef(): TypeRef<T> = TypeRef.of()
