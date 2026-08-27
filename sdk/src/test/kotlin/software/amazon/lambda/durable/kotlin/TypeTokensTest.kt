// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable.kotlin

import kotlin.test.Test
import kotlin.test.assertEquals

class TypeTokensTest {
    @Test
    fun capturesGenericTypes() {
        assertEquals("java.util.List<? extends java.lang.String>", typeToken<List<String>>().type.typeName)
    }
}
