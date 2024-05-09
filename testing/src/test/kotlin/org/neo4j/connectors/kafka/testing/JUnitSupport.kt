/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.connectors.kafka.testing

import java.lang.reflect.Parameter
import java.util.*
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.jvm.javaMethod
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock

internal object JUnitSupport {

  fun parameterContextForType(parameterType: KClass<*>): ParameterContext {
    val param = mock<Parameter> { on { type } doReturn parameterType.java }
    return mock<ParameterContext> { on { parameter } doReturn param }
  }

  inline fun <reified T : Annotation> annotatedParameterContextForType(
      parameterType: KClass<*>,
      paramAnnotation: T
  ): ParameterContext {
    val param =
        mock<Parameter> {
          on { type } doReturn parameterType.java
          on { getAnnotation(T::class.java) } doReturn paramAnnotation
        }
    return mock<ParameterContext> { on { parameter } doReturn param }
  }

  fun extensionContextFor(method: KFunction<Unit>) =
      mock<ExtensionContext> {
        on { testMethod } doReturn Optional.ofNullable(method.javaMethod)
        on { requiredTestMethod } doReturn method.javaMethod
        on { displayName } doReturn method.name
      }
}
