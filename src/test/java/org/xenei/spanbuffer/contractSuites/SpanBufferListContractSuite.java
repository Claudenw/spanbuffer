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
package org.xenei.spanbuffer.contractSuites;

import java.util.Arrays;

import org.junit.runner.RunWith;
import org.xenei.junit.contract.Contract;
import org.xenei.junit.contract.ContractImpl;
import org.xenei.junit.contract.ContractSuite;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.SpanBufferContractTest.SpanBufferProducer;
import org.xenei.spanbuffer.impl.SpanBufferList;

@RunWith(ContractSuite.class)
@ContractImpl(SpanBufferList.class)
public class SpanBufferListContractSuite {

    @Contract.Inject
    public SpanBufferProducer<SpanBufferList> getProducer() {
        return new SpanBufferProducer<SpanBufferList>() {

            @Override
            public SpanBufferList newInstance() {

                return new SpanBufferList( 0, Arrays.asList( new SpanBuffer[] {Factory.wrap( "Hello"
                        .getBytes() ), Factory.wrap( " ".getBytes() ), Factory.wrap( "World"
                                .getBytes() )} ) );
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
