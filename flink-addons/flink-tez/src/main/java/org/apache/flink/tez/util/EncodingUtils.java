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

import org.apache.flink.util.InstantiationUtil;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;

public class EncodingUtils {

	public static Object decodeObjectFromString(String encoded, ClassLoader cl) throws IOException, ClassNotFoundException {
		if (encoded == null) {
			return null;
		}
		// Using Apache Commons
		//Base64 base = new Base64();
		//byte [] bytes = base.decode(encoded);

		byte [] bytes = Base64.decodeBase64(encoded);

		return InstantiationUtil.deserializeObject(bytes, cl);

		// //byte [] bytes = BaseEncoding.base64().decode(encoded);
		//Base64.Decoder decoder = Base64.getDecoder();
		//byte [] bytes = decoder.decode(encoded);
		//return InstantiationUtil.deserializeObject(bytes, cl);
	}

	public static String encodeObjectToString(Object o) throws IOException {
		byte[] bytes = InstantiationUtil.serializeObject(o);
		// //String encoded = BaseEncoding.base64().encode(bytes);
		//Base64.Encoder encoder = Base64.getEncoder();
		//String encoded = encoder.encodeToString(bytes);

		//Base64 base = new Base64();
		//String encoded = base.encodeToString(bytes);

		String encoded = Base64.encodeBase64String(bytes);
		return encoded;

	}
}
