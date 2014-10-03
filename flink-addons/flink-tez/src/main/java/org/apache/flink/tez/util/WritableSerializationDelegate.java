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

package org.apache.flink.tez.util;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class WritableSerializationDelegate<T> implements Writable {

	//private static final Map<Thread, TypeSerializer<?>> serializers = new HashMap<Thread, TypeSerializer<?>>();

	private final TypeSerializer<T> serializer;

	private T instance;

	private DataOutput lastOutput;
	private DataInput lastInput;

	private DataOutputViewDataOutputWrapper currentOutWrapper;
	private DataInputViewDataInputWrapper currentInWrapper;

	/*
	public static void registerSerializer (TypeSerializer<?> serializer) {
		serializers.put(Thread.currentThread(), serializer);
	}
	*/



	public WritableSerializationDelegate(TypeSerializer<T> serializer) {
		this.serializer = serializer;
		this.instance = serializer.createInstance();
	}

	public void setInstance(T instance) {
		this.instance = instance;
	}

	public T getInstance() {
		return this.instance;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if (out != lastOutput) {
			lastOutput = out;
			currentOutWrapper = new DataOutputViewDataOutputWrapper(out);
		}
		serializer.serialize(instance, currentOutWrapper);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if (in != lastInput) {
			lastInput = in;
			currentInWrapper = new DataInputViewDataInputWrapper(in);
		}
		instance = serializer.deserialize(instance, currentInWrapper);
	}

	private static final class DataOutputViewDataOutputWrapper implements DataOutputView {

		private final DataOutput output;

		public DataOutputViewDataOutputWrapper(DataOutput output) {
			this.output = output;
		}

		@Override
		public void write(int b) throws IOException {
			output.write(b);
		}

		@Override
		public void write(byte[] b) throws IOException {
			output.write(b);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			output.write(b, off, len);
		}

		@Override
		public void writeBoolean(boolean v) throws IOException {
			output.writeBoolean(v);
		}

		@Override
		public void writeByte(int v) throws IOException {
			output.writeByte(v);
		}

		@Override
		public void writeShort(int v) throws IOException {
			output.writeShort(v);
		}

		@Override
		public void writeChar(int v) throws IOException {
			output.writeChar(v);
		}

		@Override
		public void writeInt(int v) throws IOException {
			output.writeInt(v);
		}

		@Override
		public void writeLong(long v) throws IOException {
			output.writeLong(v);
		}

		@Override
		public void writeFloat(float v) throws IOException {
			output.writeFloat(v);
		}

		@Override
		public void writeDouble(double v) throws IOException {
			output.writeDouble(v);
		}

		@Override
		public void writeBytes(String s) throws IOException {
			output.writeBytes(s);
		}

		@Override
		public void writeChars(String s) throws IOException {
			output.writeChars(s);
		}

		@Override
		public void writeUTF(String s) throws IOException {
			output.writeUTF(s);
		}

		@Override
		public void skipBytesToWrite(int num) throws IOException {
			for (int i = 0; i < num; i++) {
				output.write(0);
			}
		}

		@Override
		public void write(DataInputView inview, int num) throws IOException {
			for (int i = 0; i < num; i++) {
				output.write(inview.readByte());
			}
		}
	}

	private static final class DataInputViewDataInputWrapper implements DataInputView {

		private final DataInput input;


		public DataInputViewDataInputWrapper(DataInput input) {
			this.input = input;
		}

		@Override
		public void readFully(byte[] b) throws IOException {
			input.readFully(b);
		}

		@Override
		public void readFully(byte[] b, int off, int len) throws IOException {
			input.readFully(b, off, len);
		}

		@Override
		public int skipBytes(int n) throws IOException {
			return input.skipBytes(n);
		}

		@Override
		public boolean readBoolean() throws IOException {
			return input.readBoolean();
		}

		@Override
		public byte readByte() throws IOException {
			return input.readByte();
		}

		@Override
		public int readUnsignedByte() throws IOException {
			return input.readUnsignedByte();
		}

		@Override
		public short readShort() throws IOException {
			return input.readShort();
		}

		@Override
		public int readUnsignedShort() throws IOException {
			return input.readUnsignedShort();
		}

		@Override
		public char readChar() throws IOException {
			return input.readChar();
		}

		@Override
		public int readInt() throws IOException {
			return input.readInt();
		}

		@Override
		public long readLong() throws IOException {
			return input.readLong();
		}

		@Override
		public float readFloat() throws IOException {
			return input.readFloat();
		}

		@Override
		public double readDouble() throws IOException {
			return input.readDouble();
		}

		@Override
		public String readLine() throws IOException {
			return input.readLine();
		}

		@Override
		public String readUTF() throws IOException {
			return input.readUTF();
		}

		@Override
		public void skipBytesToRead(int numBytes) throws IOException {
			while (numBytes > 0) {
				numBytes -= input.skipBytes(numBytes);
			}
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			input.readFully(b, off, len);
			return len;
		}

		@Override
		public int read(byte[] b) throws IOException {
			return read(b, 0, b.length);
		}
	}
}
