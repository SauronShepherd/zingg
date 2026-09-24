# zingg-spark-connect (Spark Connect bridge)

Standalone modules providing a py4j-free Python API for Zingg, built on Apache
Spark Connect's command- and relation-plugin extension mechanisms.

## Layout

```
spark-connect/
  proto/           zingg_command.proto -- single source of truth for the wire
                   schema (Pipe, FieldDefinition, Arguments, ClientOptions,
                   ZinggCommand). `mvn generate-sources` here regenerates both
                   the Java classes (target/generated-sources/protobuf) and
                   the Python stub (python/zingg_connect/proto/zingg_command_pb2.py)
                   from the same .proto file in one step.
  server-plugin/   ZinggCommandPlugin -- a Spark Connect CommandPlugin that
                   unpacks a ZinggCommand and dispatches it into the existing
                   IZingg/ZinggOptions/SparkZFactory execution path.
                   Label submissions are delegated to the existing Java
                   SparkLabeller; no Zingg labeling/persistence logic lives in
                   Python.
  python/          zingg_connect -- a new pip package (not zingg.client) with
                   the same public method names as today's python/zingg/client.py
                   and python/zingg/pipes.py, built entirely on
                   pyspark.sql.connect's own gRPC client machinery.
```

## Why field numbers matter here

`zingg_command.proto` assigns every field an explicit tag number and documents
the rule at the top of the file: numbers are permanent once shipped, removed
fields get `reserved` instead of being deleted, and new fields always take
the next unused number. Wire compatibility depends entirely on these numbers,
never on declaration order or Python call-site argument order -- every
wrapper class in `zingg_connect` builds proto messages via named field
assignment (`args.zingg_dir = ...`, `pipe.props[name] = value`,
`args.field_definition.add().CopyFrom(...)`), never positionally, so adding a
field to `Arguments` later cannot silently reinterpret an existing one.

## What actually works today

- `mvn install` in `proto/` then `server-plugin/` builds cleanly against the
  already-installed `zingg:*:0.7.0` artifacts, for both the `spark-3.4` and
  `spark-3.5` Maven profiles (`-Dspark=3.4` / `-Dspark=3.5`, mirroring the
  root pom's profiles). The plugin interface shape was verified by
  decompiling the actual released `spark-connect_2.12` jars for 3.4.0 and
  3.5.5 (both match: a Scala trait, `Option[BoxedUnit] process(shaded Any,
  SparkConnectPlanner)` -- this differs from the plain `boolean`/`byte[]`
  shape currently on the apache/spark master branch source, so if zingg ever
  moves to Spark 4.x this signature needs re-verifying against that release's
  actual jar, not just its GitHub source).
- The Python package (`python/zingg_connect`) imports and builds real
  `ZinggCommand` proto messages end to end (see the sanity check below) using
  only `pyspark.sql.connect.client.core.SparkConnectClient` -- no py4j,
  `_jvm`, `_gateway`, `_jsparkSession`/`_jdf`, and no Databricks Connect
  sideloading anywhere in this module.
- The label submission path has been exercised locally with Spark 3.5.x and
  the Java `SparkLabeller`; a live Databricks validation remains a separate
  compatibility check.

## Interactive labeling

`CommandPlugin#process` only signals handled/not-handled back to Spark
Connect's planner -- there is no channel for it to return row data to the
client. That's fine for phases that only need success/failure (train, match,
trainMatch, link, findTrainingData, generateDocs, recommend, updateLabel),
which is why the interactive `label`/`findAndLabel` loop is split across the
two plugin types. `ZinggRelationPlugin` returns the unmarked pairs as a
queryable DataFrame; Python presents those rows and collects the user's
decisions; `ZinggCommandPlugin` sends the decisions to Java
`SparkLabeller.applyLabels`, which performs validation, training-data updates,
post-processing, and persistence. The split is a transport detail, not a
second Python implementation of the labeling logic.
