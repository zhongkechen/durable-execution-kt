package io.github.zhongkechen.durable.internal

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.HexFormat
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * Allocates stable operation identities inside one durable context.
 *
 * A nested context uses its parent operation identity as the namespace, so
 * identical local sequences in separate contexts cannot collide.
 */
internal class OperationIdSequence(
    contextId: String? = null,
) {
    private val nextNumber = AtomicInteger()
    private val namespace = contextId?.let { "$it-" }.orEmpty()
    private val allocated = ConcurrentHashMap.newKeySet<String>()

    fun next(): String {
        while (true) {
            val local = nextNumber.incrementAndGet().toString()
            if (allocated.add(local)) return digest(namespace + local)
        }
    }

    fun next(localId: String): String {
        require(localId.isNotBlank()) { "localId cannot be blank" }
        check(allocated.add(localId)) { "localId is already allocated: $localId" }
        nextNumber.incrementAndGet()
        return digest(namespace + localId)
    }

    companion object {
        fun digest(value: String): String {
            val bytes =
                MessageDigest
                    .getInstance("SHA-256")
                    .digest(value.toByteArray(StandardCharsets.UTF_8))
            return HexFormat.of().formatHex(bytes)
        }
    }
}
