package zingg.spark.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import zingg.spark.connect.proto.Arguments;
import zingg.spark.connect.proto.ClientOptions;
import zingg.spark.connect.proto.FieldDefinition;
import zingg.spark.connect.proto.Pipe;
import zingg.spark.connect.proto.LabelDecision;
import zingg.spark.connect.proto.ZinggCommand;

class ZinggCommandSchemaTest {

	@Test
	void wireFieldNumbersRemainStableAndUnique() {
		assertFieldNumbers(ZinggCommand.getDescriptor(),
				new String[] { "phase", "args", "options", "labels" }, new int[] { 1, 2, 3, 4 });
		assertFieldNumbers(LabelDecision.getDescriptor(),
				new String[] { "z_cluster", "label" }, new int[] { 1, 2 });
		assertFieldNumbers(Arguments.getDescriptor(),
				new String[] { "zingg_dir", "model_id", "job_id", "collect_metrics", "output", "data",
						"training_samples", "field_definition", "num_partitions", "label_data_sample_size",
						"threshold", "show_concise", "stop_words_cutoff", "block_size", "column" },
				new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 });
		assertFieldNumbers(ClientOptions.getDescriptor(),
				new String[] { "license", "email", "job_id", "format", "location", "column" },
				new int[] { 1, 2, 3, 4, 5, 6 });
		assertFieldNumbers(Pipe.getDescriptor(),
				new String[] { "name", "format", "preprocessors", "props", "schema", "mode", "id" },
				new int[] { 1, 2, 3, 4, 5, 6, 7 });
		assertFieldNumbers(FieldDefinition.getDescriptor(),
				new String[] { "field_name", "data_type", "match_type", "fields", "stop_words", "abbreviations" },
				new int[] { 1, 2, 3, 4, 5, 6 });
	}

	@Test
	void addingUnknownFieldsDoesNotBreakExistingMessages() throws Exception {
		ZinggCommand original = ZinggCommand.newBuilder()
				.setPhase("train")
				.setArgs(Arguments.newBuilder().setModelId("model-1"))
				.build();

		byte[] withFutureField = concat(original.toByteArray(), new byte[] { 0x20, 0x01 });
		ZinggCommand parsed = ZinggCommand.parseFrom(withFutureField);

		assertEquals("train", parsed.getPhase());
		assertEquals("model-1", parsed.getArgs().getModelId());
		assertTrue(parsed.getUnknownFields().asMap().containsKey(4));
	}

	@Test
	void labelDecisionsRoundTripAsPartOfCommand() throws Exception {
		ZinggCommand original = ZinggCommand.newBuilder()
				.setPhase("label")
				.addLabels(LabelDecision.newBuilder().setZCluster("cluster-1").setLabel(1))
				.addLabels(LabelDecision.newBuilder().setZCluster("cluster-2").setLabel(0))
				.build();

		ZinggCommand parsed = ZinggCommand.parseFrom(original.toByteArray());

		assertEquals(2, parsed.getLabelsCount());
		assertEquals("cluster-1", parsed.getLabels(0).getZCluster());
		assertEquals(1, parsed.getLabels(0).getLabel());
		assertEquals("cluster-2", parsed.getLabels(1).getZCluster());
		assertEquals(0, parsed.getLabels(1).getLabel());
	}

	private static void assertFieldNumbers(Descriptor descriptor, String[] names, int[] numbers) {
		assertEquals(names.length, numbers.length);
		Set<Integer> seen = new HashSet<>();
		for (int i = 0; i < names.length; i++) {
			FieldDescriptor field = descriptor.findFieldByName(names[i]);
			assertNotNull(field, descriptor.getFullName() + " is missing " + names[i]);
			assertEquals(numbers[i], field.getNumber(), descriptor.getFullName() + "." + names[i]);
			assertTrue(seen.add(field.getNumber()), "duplicate field number " + field.getNumber());
		}
	}

	private static byte[] concat(byte[] first, byte[] second) {
		byte[] result = new byte[first.length + second.length];
		System.arraycopy(first, 0, result, 0, first.length);
		System.arraycopy(second, 0, result, first.length, second.length);
		return result;
	}
}
