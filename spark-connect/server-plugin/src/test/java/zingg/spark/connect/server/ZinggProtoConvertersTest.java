package zingg.spark.connect.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import zingg.common.client.ClientOptions;
import zingg.common.client.FieldDefinition;
import zingg.common.client.arguments.model.Arguments;
import zingg.common.client.pipe.Pipe;
import zingg.spark.client.pipe.SparkPipe;
import zingg.spark.connect.proto.MatchType;

class ZinggProtoConvertersTest {

	@Test
	void convertsEveryPipeField() {
		zingg.spark.connect.proto.Pipe wire = zingg.spark.connect.proto.Pipe.newBuilder()
				.setName("input")
				.setFormat("csv")
				.setPreprocessors("trim")
				.putProps("path", "/data/input.csv")
				.setSchema("id string")
				.setMode("overwrite")
				.setId(7)
				.build();

		SparkPipe javaPipe = ZinggProtoConverters.toJavaPipe(wire);

		assertEquals("input", javaPipe.getName());
		assertEquals("csv", javaPipe.getFormat());
		assertEquals("trim", javaPipe.getPreprocessors());
		assertEquals("/data/input.csv", javaPipe.get("path"));
		assertEquals("id string", javaPipe.getSchema());
		assertEquals("overwrite", javaPipe.getMode());
		assertEquals(7, javaPipe.getId());
	}

	@Test
	void convertsEveryFieldDefinitionField() {
		zingg.spark.connect.proto.FieldDefinition wire = zingg.spark.connect.proto.FieldDefinition.newBuilder()
				.setFieldName("name")
				.setDataType("string")
				.addMatchType(MatchType.newBuilder().setName("FUZZY"))
				.setFields("first_name,last_name")
				.setStopWords("the,a")
				.setAbbreviations("intl=international")
				.build();

		FieldDefinition javaField = ZinggProtoConverters.toJavaFieldDefinition(wire);

		assertEquals("name", javaField.getFieldName());
		assertEquals("string", javaField.getDataType());
		assertEquals("first_name,last_name", javaField.getFields());
		assertEquals("the,a", javaField.getStopWords());
		assertEquals("intl=international", javaField.getAbbreviations());
		assertEquals(1, javaField.getMatchType().size());
		assertEquals("FUZZY", javaField.getMatchType().get(0).getName());
	}

	@Test
	void convertsArgumentsAndClientOptionsWithoutDroppingValues() throws Throwable {
		zingg.spark.connect.proto.Arguments wireArgs = zingg.spark.connect.proto.Arguments.newBuilder()
				.setZinggDir("/models")
				.setModelId("customer")
				.setJobId(42)
				.setCollectMetrics(true)
				.setNumPartitions(8)
				.setLabelDataSampleSize(0.25f)
				.setThreshold(0.8)
				.setShowConcise(true)
				.setStopWordsCutoff(0.15f)
				.setBlockSize(1000)
				.setColumn("z_cluster")
				.build();

		Arguments javaArgs = ZinggProtoConverters.toJavaArguments(wireArgs);

		assertEquals("/models", javaArgs.getZinggDir());
		assertEquals("customer", javaArgs.getModelId());
		assertEquals(42, javaArgs.getJobId());
		assertTrue(javaArgs.getCollectMetrics());
		assertEquals(8, javaArgs.getNumPartitions());
		assertEquals(0.25f, javaArgs.getLabelDataSampleSize());
		assertTrue(javaArgs.getShowConcise());
		assertEquals(0.15f, javaArgs.getStopWordsCutoff());
		assertEquals(1000, javaArgs.getBlockSize());
		assertEquals("z_cluster", javaArgs.getColumn());

		zingg.spark.connect.proto.ClientOptions wireOptions = zingg.spark.connect.proto.ClientOptions.newBuilder()
				.setLicense("license.txt")
				.setEmail("user@example.com")
				.setJobId("job-7")
				.setFormat("csv")
				.setLocation("/exports")
				.setColumn("z_cluster")
				.build();

		ClientOptions javaOptions = ZinggProtoConverters.toJavaClientOptions("match", wireOptions);

		assertNotNull(javaOptions.getOptionMaster().get(ClientOptions.PHASE));
		assertTrue(javaOptions.getCommandLineArgs().length > 0);
		assertContains(javaOptions, ClientOptions.PHASE, "match");
		assertContains(javaOptions, ClientOptions.LICENSE, "license.txt");
		assertContains(javaOptions, ClientOptions.EMAIL, "user@example.com");
		assertContains(javaOptions, ClientOptions.JOBID, "job-7");
		assertContains(javaOptions, ClientOptions.FORMAT, "csv");
		assertContains(javaOptions, ClientOptions.LOCATION, "/exports");
		assertContains(javaOptions, ClientOptions.COLUMN, "z_cluster");
	}

	private static void assertContains(ClientOptions options, String name, String value) {
		String[] args = options.getCommandLineArgs();
		for (int i = 0; i + 1 < args.length; i++) {
			if (name.equals(args[i])) {
				assertEquals(value, args[i + 1]);
				return;
			}
		}
		throw new AssertionError("Missing " + name + " in converted options");
	}
}
