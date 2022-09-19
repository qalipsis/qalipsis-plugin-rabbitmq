/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package io.qalipsis.plugins.rabbitmq.consumer.converter

import com.rabbitmq.client.Delivery
import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.messaging.deserializer.MessageDeserializer
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.plugins.rabbitmq.consumer.RabbitMqConsumerRecord
import java.util.concurrent.atomic.AtomicLong

/**
 * Implementation of [DatasourceObjectConverter], that reads a message of native RabbitMQ records and forwards each of
 * them converted as [RabbitMqConsumerRecord].
 *
 * @author Gabriel Moraes
 */
internal class RabbitMqConsumerConverter<V>(
    private val valueDeserializer: MessageDeserializer<V>
) : DatasourceObjectConverter<Delivery, RabbitMqConsumerRecord<V>> {

    override suspend fun supply(
        offset: AtomicLong, value: Delivery,
        output: StepOutput<RabbitMqConsumerRecord<V>>
    ) {
        tryAndLogOrNull(log) {
            output.send(
                RabbitMqConsumerRecord(
                    offset.getAndIncrement(),
                    value.envelope,
                    value.properties,
                    valueDeserializer.deserialize(value.body)
                )
            )
        }
    }

    companion object {
        @JvmStatic
        private val log = logger()
    }
}
