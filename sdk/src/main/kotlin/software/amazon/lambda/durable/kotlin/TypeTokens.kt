// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable.kotlin

import software.amazon.lambda.durable.TypeToken

public inline fun <reified T> typeToken(): TypeToken<T> = object : TypeToken<T>() {}
