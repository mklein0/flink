/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateBackendMigrationTestBase;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.testutils.statemigration.TestType;
import org.apache.flink.runtime.testutils.statemigration.TestType.V1TestTypeSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * Tests for {@link RocksDBMapState} iterators.
 *
 * <p>This is an adaptation of {@link StateBackendMigrationTestBase} &
 * {@link RocksDBStateBackendMigrationTest} tests
 */
public class RocksDBMapStateIteratorTest {
	@Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

	// Store it because we need it for the cleanup test.
	private String dbPath;

	public RocksDBMapStateIteratorTest() {}

	protected RocksDBStateBackend getStateBackend() throws IOException {
		dbPath = tempFolder.newFolder().getAbsolutePath();
		String checkpointPath = tempFolder.newFolder().toURI().toString();
		final boolean enableIncrementalCheckpointing = false;
		RocksDBStateBackend backend =
			new RocksDBStateBackend(
				new FsStateBackend(checkpointPath), enableIncrementalCheckpointing);

		Configuration configuration = new Configuration();
		configuration.setString(
			RocksDBOptions.TIMER_SERVICE_FACTORY,
			RocksDBStateBackend.PriorityQueueStateType.ROCKSDB.toString());
		backend = backend.configure(configuration, Thread.currentThread().getContextClassLoader());
		backend.setDbStoragePath(dbPath);
		return backend;
	}

	private <K> AbstractKeyedStateBackend<K> createKeyedBackend(TypeSerializer<K> keySerializer)
		throws Exception {
		return this.createKeyedBackend(keySerializer, new DummyEnvironment());
	}

	private <K> AbstractKeyedStateBackend<K> createKeyedBackend(
		TypeSerializer<K> keySerializer, Environment env) throws Exception {
		return this.createKeyedBackend(keySerializer, 10, new KeyGroupRange(0, 9), env);
	}

	private <K> AbstractKeyedStateBackend<K> createKeyedBackend(
		TypeSerializer<K> keySerializer,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		Environment env)
		throws Exception {
		return this.getStateBackend()
			.createKeyedStateBackend(
				env,
				new JobID(),
				"test_op",
				keySerializer,
				numberOfKeyGroups,
				keyGroupRange,
				env.getTaskKvStateRegistry(),
				TtlTimeProvider.DEFAULT,
				new UnregisteredMetricsGroup(),
				Collections.emptyList(),
				new CloseableRegistry());
	}

	@Test
	public void testKeyedMapStateIntegerIteration() throws Exception {

		MapStateDescriptor<Integer, TestType> initialAccessDescriptor =
			new MapStateDescriptor<>("test-name", IntSerializer.INSTANCE, new V1TestTypeSerializer());
		AbstractKeyedStateBackend<Integer> backend = this.createKeyedBackend(IntSerializer.INSTANCE);

		try {
			MapState<Integer, TestType> mapState =
				backend.getPartitionedState(
					VoidNamespace.INSTANCE,
					StateBackendMigrationTestBase.CustomVoidNamespaceSerializer.INSTANCE,
					initialAccessDescriptor);

			backend.setCurrentKey(1);
			mapState.put(1, new TestType("key-1", 1));
			mapState.put(2, new TestType("key-1", 2));
			mapState.put(3, new TestType("key-1", 3));

			backend.setCurrentKey(2);
			mapState.put(1, new TestType("key-2", 1));

			backend.setCurrentKey(3);
			mapState.put(1, new TestType("key-3", 1));
			mapState.put(2, new TestType("key-3", 2));
			mapState.put(3, new TestType("key-3", 3));
			mapState.put(4, new TestType("key-3", 4));
			mapState.put(5, new TestType("key-3", 5));

			// Reset partition key & iterate through key/values
			backend.setCurrentKey(1);
			Iterator<Entry<Integer, TestType>> iterable1 = mapState.iterator();
			Entry<Integer, TestType> actual = iterable1.next();
			Assert.assertEquals(1, (long) actual.getKey());
			Assert.assertEquals(new TestType("key-1", 1), actual.getValue());
			actual = iterable1.next();
			Assert.assertEquals(2, (long) actual.getKey());
			Assert.assertEquals(new TestType("key-1", 2), actual.getValue());
			actual = iterable1.next();
			Assert.assertEquals(3, (long) actual.getKey());
			Assert.assertEquals(new TestType("key-1", 3), actual.getValue());
			Assert.assertFalse(iterable1.hasNext());
			mapState.put(123, new TestType("new-key-1", 123));

			// Reset partition key & iterate through key/values
			backend.setCurrentKey(3);
			RocksDBMapState<?, ?, Integer, TestType> smMapState =
				(RocksDBMapState<?, ?, Integer, TestType>) mapState;

			try (CloseableIterator<Entry<Integer, TestType>> iterable2 =
				smMapState.forwardIterator(3, 4)) {
				actual = iterable2.next();
				Assert.assertEquals(3, (long) actual.getKey());
				Assert.assertEquals(new TestType("key-3", 3), actual.getValue());
				Assert.assertFalse(iterable2.hasNext());
			}

			try (CloseableIterator<Entry<Integer, TestType>> iterable2 =
				smMapState.reversedIterator(3, 5)) {
				actual = iterable2.next();
				Assert.assertEquals(4, (long) actual.getKey());
				Assert.assertEquals(new TestType("key-3", 4), actual.getValue());
				actual = iterable2.next();
				Assert.assertEquals(3, (long) actual.getKey());
				Assert.assertEquals(new TestType("key-3", 3), actual.getValue());
				Assert.assertFalse(iterable2.hasNext());
			}

		} finally {
			backend.dispose();
		}
	}

	@Test
	public void testKeyedMapStateTuple3Iteration() throws Exception {
		// TupleX structures do not support null values.
		final TypeInformation<Tuple3<String, String, String>> keySchema =
			TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {});
		final TypeSerializer<Tuple3<String, String, String>> keySchemaSerializer =
			keySchema.createSerializer(new ExecutionConfig());

		final MapStateDescriptor<Tuple3<String, String, String>, TestType> initialAccessDescriptor =
			new MapStateDescriptor<>("test-name", keySchemaSerializer, new V1TestTypeSerializer());

		// Stream by Key Type  = Int, sub Map Key = tuple3
		AbstractKeyedStateBackend<Integer> backend = this.createKeyedBackend(IntSerializer.INSTANCE);

		try {
			MapState<Tuple3<String, String, String>, TestType> mapState =
				backend.getPartitionedState(
					VoidNamespace.INSTANCE,
					StateBackendMigrationTestBase.CustomVoidNamespaceSerializer.INSTANCE,
					initialAccessDescriptor);

			backend.setCurrentKey(1);
			mapState.put(Tuple3.of("1st-1", "2nd-1", "3rd-1"), new TestType("key-1", 1));
			mapState.put(Tuple3.of("1st-1", "2nd-2", "3rd-1"), new TestType("key-1", 2));
			mapState.put(Tuple3.of("1st-1", "2nd-3", "3rd-1"), new TestType("key-1", 3));
			mapState.put(Tuple3.of("1st-1", "", "3rd-1"), new TestType("key-1", 0));

			backend.setCurrentKey(2);
			mapState.put(Tuple3.of("1st-2", "2nd-1", "3rd-1"), new TestType("key-2", 1));

			backend.setCurrentKey(3);
			mapState.put(Tuple3.of("1st-3", "2nd-1", "3rd-1"), new TestType("key-3", 1));
			mapState.put(Tuple3.of("1st-3", "2nd-2", "3rd-2"), new TestType("key-3", 2));
			mapState.put(Tuple3.of("1st-3", "2nd-2", "3rd-3"), new TestType("key-3", 3));
			mapState.put(Tuple3.of("1st-3", "2nd-2", "3rd-4"), new TestType("key-3", 4));
			mapState.put(Tuple3.of("1st-3", "2nd-3", "3rd-5"), new TestType("key-3", 5));

			// Reset partition key & iterate through key/values
			backend.setCurrentKey(1);
			Iterator<Entry<Tuple3<String, String, String>, TestType>> iterable1 = mapState.iterator();
			Entry<Tuple3<String, String, String>, TestType> actual = iterable1.next();
			Assert.assertEquals(Tuple3.of("1st-1", "", "3rd-1"), actual.getKey());
			Assert.assertEquals(new TestType("key-1", 0), actual.getValue());
			actual = iterable1.next();
			Assert.assertEquals(Tuple3.of("1st-1", "2nd-1", "3rd-1"), actual.getKey());
			Assert.assertEquals(new TestType("key-1", 1), actual.getValue());
			actual = iterable1.next();
			Assert.assertEquals(Tuple3.of("1st-1", "2nd-2", "3rd-1"), actual.getKey());
			Assert.assertEquals(new TestType("key-1", 2), actual.getValue());
			actual = iterable1.next();
			Assert.assertEquals(Tuple3.of("1st-1", "2nd-3", "3rd-1"), actual.getKey());
			Assert.assertEquals(new TestType("key-1", 3), actual.getValue());
			Assert.assertFalse(iterable1.hasNext());
			mapState.put(Tuple3.of("1st-1", "2nd-123", "3rd-1"), new TestType("new-key-1", 123));

			// Reset partition key & iterate through key/values
			backend.setCurrentKey(3);
			RocksDBMapState<?, ?, Tuple3<String, String, String>, TestType> smMapState =
				(RocksDBMapState<?, ?, Tuple3<String, String, String>, TestType>) mapState;

			Tuple3<String, String, String> lowerBound = Tuple3.of("1st-3", "2nd-2", "3rd-3");
			try (CloseableIterator<Entry<Tuple3<String, String, String>, TestType>> iterable2 =
				smMapState.forwardIterator(lowerBound, Tuple3.of("1st-3", "2nd-2", "3rd-4"))) {

				actual = iterable2.next();
				Assert.assertEquals(lowerBound, actual.getKey());
				Assert.assertEquals(new TestType("key-3", 3), actual.getValue());
				Assert.assertFalse(iterable2.hasNext());
			}

			lowerBound = Tuple3.of("1st-3", "2nd-2", "");
			try (CloseableIterator<Entry<Tuple3<String, String, String>, TestType>> iterable3 =
				smMapState.forwardIterator(lowerBound, Tuple3.of("1st-3", "2nd-2", "3rd-4"))) {

				actual = iterable3.next();
				Assert.assertEquals(Tuple3.of("1st-3", "2nd-2", "3rd-2"), actual.getKey());
				Assert.assertEquals(new TestType("key-3", 2), actual.getValue());
				actual = iterable3.next();
				Assert.assertEquals(Tuple3.of("1st-3", "2nd-2", "3rd-3"), actual.getKey());
				Assert.assertEquals(new TestType("key-3", 3), actual.getValue());
				Assert.assertFalse(iterable3.hasNext());
			}

		} finally {
			backend.dispose();
		}
	}

	@Test
	public void testKeyedMapStateRowIteration() throws Exception {

		// Row should support null values.  Null sorts to the end.
		final TypeInformation<Row> rowSchema = Types.ROW_NAMED(
			new String[] {"f1", "f2", "f3"},
			Types.INT, Types.BOOLEAN, Types.STRING);
		final TypeSerializer<Row> rowSchemaSerializer =
			rowSchema.createSerializer(new ExecutionConfig());

		MapStateDescriptor<Row, TestType> initialAccessDescriptor =
			new MapStateDescriptor<>("test-name", rowSchemaSerializer, new V1TestTypeSerializer());

		// Stream by Key Type  = Int, sub Map Key = tuple3
		AbstractKeyedStateBackend<Integer> backend = this.createKeyedBackend(IntSerializer.INSTANCE);

		try {
			MapState<Row, TestType> mapState =
				backend.getPartitionedState(
					VoidNamespace.INSTANCE,
					StateBackendMigrationTestBase.CustomVoidNamespaceSerializer.INSTANCE,
					initialAccessDescriptor);

			backend.setCurrentKey(1);
			mapState.put(Row.of(1, true, "3rd-1"), new TestType("key-1", 1));
			mapState.put(Row.of(1, true, "3rd-2"), new TestType("key-1", 2));
			mapState.put(Row.of(1, true, "3rd-3"), new TestType("key-1", 3));
			mapState.put(Row.of(1, false, "3rd-0"), new TestType("key-1", 0));
			mapState.put(Row.of(1, null, "3rd-4"), new TestType("key-1", 4));

			backend.setCurrentKey(2);
			mapState.put(Row.of(2, false, "3rd-1"), new TestType("key-2", 1));

			backend.setCurrentKey(3);
			mapState.put(Row.of(3, false, "3rd-1"), new TestType("key-3", 1));
			mapState.put(Row.of(3, false, "3rd-2"), new TestType("key-3", 2));
			mapState.put(Row.of(3, true, "3rd-3"), new TestType("key-3", 3));
			mapState.put(Row.of(3, true, "3rd-4"), new TestType("key-3", 4));
			mapState.put(Row.of(3, true, "3rd-5"), new TestType("key-3", 5));

			// Reset partition key & iterate through key/values
			backend.setCurrentKey(1);
			Iterator<Entry<Row, TestType>> iterable1 = mapState.iterator();
			Entry<Row, TestType> actual = iterable1.next();
			Assert.assertEquals(Row.of(1, false, "3rd-0"), actual.getKey());
			Assert.assertEquals(new TestType("key-1", 0), actual.getValue());
			actual = iterable1.next();
			Assert.assertEquals(Row.of(1, true, "3rd-1"), actual.getKey());
			Assert.assertEquals(new TestType("key-1", 1), actual.getValue());
			actual = iterable1.next();
			Assert.assertEquals(Row.of(1, true, "3rd-2"), actual.getKey());
			Assert.assertEquals(new TestType("key-1", 2), actual.getValue());
			actual = iterable1.next();
			Assert.assertEquals(Row.of(1, true, "3rd-3"), actual.getKey());
			Assert.assertEquals(new TestType("key-1", 3), actual.getValue());
			actual = iterable1.next();
			Assert.assertEquals(Row.of(1, null, "3rd-4"), actual.getKey());
			Assert.assertEquals(new TestType("key-1", 4), actual.getValue());
			Assert.assertFalse(iterable1.hasNext());
			mapState.put(Row.of(1, false, "3rd-123"), new TestType("new-key-1", 123));

			// Convert MapState instance to original RocksDB Class to access advanced methods.
			RocksDBMapState<?, ?, Row, TestType> smMapState =
				(RocksDBMapState<?, ?, Row, TestType>) mapState;

			try (CloseableIterator<Entry<Row, TestType>> iterator2 = smMapState.forwardIterator()) {
				actual = iterator2.next();
				Assert.assertEquals(Row.of(1, false, "3rd-0"), actual.getKey());
				Assert.assertEquals(new TestType("key-1", 0), actual.getValue());

				// New entry from before
				actual = iterator2.next();
				Assert.assertEquals(Row.of(1, false, "3rd-123"), actual.getKey());
				Assert.assertEquals(new TestType("new-key-1", 123), actual.getValue());

				actual = iterator2.next();
				Assert.assertEquals(Row.of(1, true, "3rd-1"), actual.getKey());
				Assert.assertEquals(new TestType("key-1", 1), actual.getValue());
				actual = iterator2.next();
				Assert.assertEquals(Row.of(1, true, "3rd-2"), actual.getKey());
				Assert.assertEquals(new TestType("key-1", 2), actual.getValue());
				actual = iterator2.next();
				Assert.assertEquals(Row.of(1, true, "3rd-3"), actual.getKey());
				Assert.assertEquals(new TestType("key-1", 3), actual.getValue());
				actual = iterator2.next();
				Assert.assertEquals(Row.of(1, null, "3rd-4"), actual.getKey());
				Assert.assertEquals(new TestType("key-1", 4), actual.getValue());
				Assert.assertFalse(iterator2.hasNext());

				// See if overwriting entry with same value causes issue.
				mapState.put(Row.of(1, false, "3rd-123"), new TestType("new-key-1", 123));
			}

			// Reset partition key & iterate through key/values
			backend.setCurrentKey(3);

			Row lowerBound = Row.of(3, true, "3rd-3");
			try (CloseableIterator<Entry<Row, TestType>> iterable2 =
				smMapState.forwardIterator(lowerBound, Row.of(3, true, "3rd-4"))) {

				actual = iterable2.next();
				Assert.assertEquals(lowerBound, actual.getKey());
				Assert.assertEquals(new TestType("key-3", 3), actual.getValue());
				Assert.assertFalse(iterable2.hasNext());
			}

			// Null sorts to the end
			try (CloseableIterator<Entry<Row, TestType>> iterator4 =
				smMapState.forwardIterator()) {

				actual = iterator4.next();
				Assert.assertEquals(Row.of(3, false, "3rd-1"), actual.getKey());
				Assert.assertEquals(new TestType("key-3", 1), actual.getValue());
				actual = iterator4.next();
				Assert.assertEquals(Row.of(3, false, "3rd-2"), actual.getKey());
				Assert.assertEquals(new TestType("key-3", 2), actual.getValue());
				actual = iterator4.next();
				Assert.assertEquals(Row.of(3, true, "3rd-3"), actual.getKey());
				Assert.assertEquals(new TestType("key-3", 3), actual.getValue());
				actual = iterator4.next();
				Assert.assertEquals(Row.of(3, true, "3rd-4"), actual.getKey());
				Assert.assertEquals(new TestType("key-3", 4), actual.getValue());
				actual = iterator4.next();
				Assert.assertEquals(Row.of(3, true, "3rd-5"), actual.getKey());
				Assert.assertEquals(new TestType("key-3", 5), actual.getValue());

				Assert.assertFalse(iterator4.hasNext());
			}

			lowerBound = Row.of(3, true, null);
			try (CloseableIterator<Entry<Row, TestType>> iterator5 =
				smMapState.forwardIterator(lowerBound, null)) {
				Assert.assertFalse(iterator5.hasNext());
			}

		} finally {
			backend.dispose();
		}
	}
}
