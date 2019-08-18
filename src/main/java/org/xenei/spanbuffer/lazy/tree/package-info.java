/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Classes that lazily load portions of a span buffer using a tree structure to
 * locate the buffers. The lazily loaded buffers may be disposed and re-read as
 * needed.
 * 
 * Tree Serialization is generally used to send large blocks of data over
 * smaller protocols (e.g. sending large data objects over MQTT or Kafka
 * queues). In the tree structure the leaf nodes contain the data while the
 * inner nodes contain pointers to other inner nodes or leaf nodes.
 * 
 * Serialization and Deserialization of buffers is handled by the classes in the
 * org.xenei.spanbuffer.lazy.tree.serde package.
 * 
 */
package org.xenei.spanbuffer.lazy.tree;
