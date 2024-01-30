/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import kotlin.jvm.optionals.getOrNull
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.platform.commons.support.AnnotationSupport

internal object AnnotationSupport {

  inline fun <reified T : Annotation> findAnnotation(context: ExtensionContext?): T? {
    var current = context
    while (current != null) {
      val methodAnnotation =
          AnnotationSupport.findAnnotation(
              current.requiredTestMethod,
              T::class.java,
          )
      if (methodAnnotation.isPresent) {
        return methodAnnotation.get()
      }
      val classAnnotation =
          AnnotationSupport.findAnnotation(current.requiredTestClass, T::class.java)
      if (classAnnotation.isPresent) {
        return classAnnotation.get()
      }
      current = current.parent.getOrNull()
    }
    return null
  }
}
