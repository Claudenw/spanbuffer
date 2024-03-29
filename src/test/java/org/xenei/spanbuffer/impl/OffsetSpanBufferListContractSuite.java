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
package org.xenei.spanbuffer.impl;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.runner.RunWith;
import org.xenei.junit.contract.Contract;
import org.xenei.junit.contract.ContractImpl;
import org.xenei.junit.contract.ContractSuite;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.SpanBufferContractTest.SpanBufferProducer;

@RunWith(ContractSuite.class)
@ContractImpl(SpanBufferList.class)
public class OffsetSpanBufferListContractSuite {

    private SpanBuffer hello;
    private SpanBuffer space;
    private SpanBuffer world;

    public OffsetSpanBufferListContractSuite() {
        ByteBuffer bb = ByteBuffer.wrap("xHellox".getBytes());
        bb.limit(6);
        bb.position(1);
        hello = new SpanByteBuffer(bb);
        bb = ByteBuffer.wrap("y ".getBytes());
        bb.position(1);
        space = new SpanByteBuffer(bb);
        bb = ByteBuffer.wrap("zWorldz".getBytes());
        bb.limit(6);
        bb.position(1);
        world = new SpanByteBuffer(bb);
    }

    @Contract.Inject
    public SpanBufferProducer<SpanBufferList> getProducer() {
        return new SpanBufferProducer<SpanBufferList>() {

            @Override
            public SpanBufferList newInstance() {

                return new SpanBufferList(0, Arrays.asList(new SpanBuffer[] { hello, space, world }));
            }

            @Override
            public void cleanUp() {
                // do nothing
            }

            @Override
            public byte[] getBufferContents() {
                return "Hello World".getBytes();
            }
        };
    }
}
