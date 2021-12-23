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
package org.xenei.spanbuffer.lazy.tree.serde;

import org.xenei.spanbuffer.lazy.tree.node.BufferFactory;

/**
 * Basic implementation of the Serde.
 *
 * This is a convenience class to keep the serializer, deserializer and buffer
 * factory together in one place.
 *
 * @param <P> the Position implementation.
 */
public class SerdeImpl<P extends Position> extends AbstractSerde<P> {
    private TreeDeserializer<P> deserializer;
    private TreeSerializer<P> serializer;
    private BufferFactory factory;

    /**
     * Constructor. This constructor calls Serde.verify to verify that the
     * components work together correctly.
     *
     * @param factory      The factory for the serde.
     * @param serializer   the serializer for the serde
     * @param deserializer the deserializer for the serde.
     * @see AbstractSerde#verify()
     */
    public SerdeImpl(BufferFactory factory, TreeSerializer<P> serializer, TreeDeserializer<P> deserializer) {
        this.factory = factory;
        this.serializer = serializer;
        this.deserializer = deserializer;
        verify();
    }

    @Override
    public TreeDeserializer<P> getDeserializer() {
        return deserializer;
    }

    @Override
    public TreeSerializer<P> getSerializer() {
        return serializer;
    }

    @Override
    public BufferFactory getFactory() {
        return factory;
    }

}
