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
package org.xenei.spanbuffer.lazy.linear;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.runner.RunWith;
import org.xenei.junit.contract.Contract;
import org.xenei.junit.contract.ContractImpl;
import org.xenei.junit.contract.ContractSuite;
import org.xenei.spanbuffer.AbstractSpanBuffer;
import org.xenei.spanbuffer.Factory;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.SpanBufferContractTest.SpanBufferProducer;

@RunWith(ContractSuite.class)
@ContractImpl(OnHeapLazyLoaderContractSuite.TestingSpanBuffer.class)
public class OnHeapLazyLoaderContractSuite {

	private File file;
	private byte[] contents;
	private List<RandomAccessFile> lst;

	public OnHeapLazyLoaderContractSuite() throws FileNotFoundException, IOException {
		URL url = this.getClass().getClassLoader().getResource("small.txt");
		file = new File(url.getFile());
		lst = new ArrayList<RandomAccessFile>();
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
				FileInputStream fis = new FileInputStream(file)) {
			IOUtils.copyLarge(fis, baos);
			contents = baos.toByteArray();
		}
	}

	@Contract.Inject
	public SpanBufferProducer<SpanBuffer> getProducer() {
		return new SpanBufferProducer<SpanBuffer>() {

			@Override
			public SpanBuffer newInstance() {
				try {
					RandomAccessFile raf = new RandomAccessFile(file, "r");
					lst.add(raf);
					return OnHeapLazyLoader.load(raf, Factory.DEFAULT_INTERNAL_BUFFER_SIZE, false);
				} catch (IOException e) {
					throw new IllegalStateException(e);
				}
			}

			@Override
			public void cleanUp() {
				for (RandomAccessFile raf : lst) {
					try {
						raf.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

			@Override
			public byte[] getBufferContents() {
				return contents;
			}
		};
	}

	private static class TestingSpanBuffer extends AbstractSpanBuffer {

		protected TestingSpanBuffer(long offset) {
			super(offset);
		}

		@Override
		public SpanBuffer duplicate(long newOffset) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public SpanBuffer sliceAt(long position) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public SpanBuffer head(long byteCount) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public byte read(long position) throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int read(long position, byte[] buff, int pos, int len) throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public long getLength() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public long getEnd() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int read(long position, ByteBuffer buff) throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

	}
}
