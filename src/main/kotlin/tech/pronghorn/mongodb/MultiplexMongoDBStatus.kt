/*
 * Copyright 2017 Pronghorn Technology LLC
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

package tech.pronghorn.mongodb

import com.mongodb.connection.AsyncCompletionHandler
import org.bson.ByteBuf

internal sealed class MultiplexMongoDBStatus

internal object Idle : MultiplexMongoDBStatus()

internal class HeaderOutstanding(internal val handler: AsyncCompletionHandler<ByteBuf>) : MultiplexMongoDBStatus()

internal object HeaderReturned : MultiplexMongoDBStatus()

internal class DataOutstanding(internal val numBytes: Int,
                               internal val handler: AsyncCompletionHandler<ByteBuf>) : MultiplexMongoDBStatus()
