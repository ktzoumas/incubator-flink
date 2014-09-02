/**
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

package org.apache.flink.api.java.typeutils;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.InvalidTypesException;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.runtime.GenericArraySerializer;
import org.apache.flink.types.TypeInformation;

public class ObjectArrayTypeInfo<T, C> extends TypeInformation<T> {

	private final Type arrayType;
	private final Type componentType;
	private final TypeInformation<C> componentInfo;

	@SuppressWarnings("unchecked")
	private ObjectArrayTypeInfo(Type arrayType, Type componentType) {
		this.arrayType = arrayType;
		this.componentType = componentType;
		this.componentInfo = (TypeInformation<C>) TypeExtractor.createTypeInfo(componentType);
	}
	
	private ObjectArrayTypeInfo(Type arrayType, Type componentType, TypeInformation<C> componentInfo) {
		this.arrayType = arrayType;
		this.componentType = componentType;
		this.componentInfo = componentInfo;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 1;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<T> getTypeClass() {
		return (Class<T>) this.arrayType.getClass();
	}

	public Type getType() {
		return arrayType;
	}

	public Type getComponentType() {
		return this.componentType;
	}
	
	public TypeInformation<C> getComponentInfo() {
		return componentInfo;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public TypeSerializer<T> createSerializer() {
		// use raw type for serializer if generic array type
		if (this.componentType instanceof GenericArrayType) {
			ParameterizedType paramType = (ParameterizedType) ((GenericArrayType) this.componentType).getGenericComponentType();
			
			return (TypeSerializer<T>) new GenericArraySerializer<C>((Class<C>) paramType.getRawType(),
					this.componentInfo.createSerializer());
		} else {
			return (TypeSerializer<T>) new GenericArraySerializer<C>((Class<C>) this.componentType, this.componentInfo.createSerializer());
		}
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "<" + this.componentInfo + ">";
	}

	// --------------------------------------------------------------------------------------------

	public static <T, C> ObjectArrayTypeInfo<T, C> getInfoFor(Type type, TypeInformation<C> componentInfo) {
		
		// generic array type e.g. for Tuples
		if (type instanceof GenericArrayType) {
			GenericArrayType genericArray = (GenericArrayType) type;
			return new ObjectArrayTypeInfo<T, C>(type, genericArray.getGenericComponentType(), componentInfo);
		}
		// for tuples without generics (e.g. generated by the TypeInformation parser)
		else if (type instanceof Class<?> && ((Class<?>) type).isArray() && componentInfo.isTupleType()
				&& type != Tuple.class) {
			return new ObjectArrayTypeInfo<T, C>(type, ((Class<?>) type).getComponentType(), componentInfo);
		}
		return getInfoFor(type);
	}
	
	@SuppressWarnings("unchecked")
	public static <T, C> ObjectArrayTypeInfo<T, C> getInfoFor(Type type) {
		// class type e.g. for custom objects
		if (type instanceof Class<?> && ((Class<?>) type).isArray() && BasicTypeInfo.getInfoFor((Class<C>) type) == null) {
			Class<C> array = (Class<C>) type;
			return new ObjectArrayTypeInfo<T, C>(type, array.getComponentType());
		}
		throw new InvalidTypesException("The given type is not a valid object array.");
	}
}
