/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.spark.structure.io.gryo;

import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import scala.collection.Seq;
import scala.collection.immutable.ArraySeq;

import java.util.function.Function;

/**
 * Serializer for Scala's reference-array wrapper sequences. In Scala 2.13 (the Scala version Spark is built against)
 * wrapping a reference array produces an {@code immutable.ArraySeq.ofRef}, which replaced the {@code WrappedArray}
 * used under Scala 2.12. Scala 2.13 also retains {@code mutable.ArraySeq} as a distinct type -- it is what the
 * deprecated {@code scala.collection.mutable.WrappedArray} type/val alias resolves to. Both variants share the same
 * {@code apply(int)}/{@code size()} shape via {@link Seq}, so a single implementation is reused for both, exposed as
 * two instances so each concrete {@code .ofRef} class can be registered with Kryo under its own type. This is
 * defensive coverage in case a mismatched Spark/Scala build on the classpath at runtime produces the Scala
 * 2.12-style mutable variant instead of the immutable one Spark 4/Scala 2.13 normally produces.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class WrappedArraySerializer<T> extends Serializer<Seq<T>> {

    /**
     * Serializer for {@code scala.collection.immutable.ArraySeq.ofRef} -- the type Spark 4/Scala 2.13 normally
     * produces when wrapping a reference array.
     */
    public static final WrappedArraySerializer<Object> IMMUTABLE =
            new WrappedArraySerializer<>(array -> new ArraySeq.ofRef<>(array));

    /**
     * Serializer for {@code scala.collection.mutable.ArraySeq.ofRef} -- the type that the deprecated
     * {@code scala.collection.mutable.WrappedArray} alias resolves to.
     */
    public static final WrappedArraySerializer<Object> MUTABLE =
            new WrappedArraySerializer<>(array -> new scala.collection.mutable.ArraySeq.ofRef<>(array));

    private final Function<T[], Seq<T>> constructor;

    /**
     * No-arg constructor retained for backward compatibility (e.g. reflective instantiation). Defaults to producing
     * {@code immutable.ArraySeq.ofRef} instances, matching this class's original behavior.
     */
    public WrappedArraySerializer() {
        this(array -> new ArraySeq.ofRef<>(array));
    }

    private WrappedArraySerializer(final Function<T[], Seq<T>> constructor) {
        this.constructor = constructor;
    }

    @Override
    public void write(final Kryo kryo, final Output output, final Seq<T> iterable) {
        output.writeVarInt(iterable.size(), true);
        for (int i = 0; i < iterable.size(); i++) {
            kryo.writeClassAndObject(output, iterable.apply(i));
        }
    }

    @Override
    public Seq<T> read(final Kryo kryo, final Input input, final Class<Seq<T>> aClass) {
        final int size = input.readVarInt(true);
        final T[] array = (T[]) new Object[size];
        for (int i = 0; i < size; i++) {
            array[i] = (T) kryo.readClassAndObject(input);
        }
        return this.constructor.apply(array);
    }
}
