# Zingg Databricks bundle

This directory contains the Databricks Declarative Automation Bundle resources for deploying Zingg to Databricks.

The bundle uses the direct deployment engine and creates only ephemeral single-node job compute. It does not create a persistent interactive cluster.

## What is included

- `zingg`: generic Spark JAR job that launches `zingg.spark.client.SparkClient` and accepts Zingg `--phase` and `--conf` arguments.
- `zingg_demo`: job that runs the repository's existing `examples/databricks/FebrlExample.ipynb` notebook.
- A normalized `dist/zingg.jar` built from the Zingg assembly.
- A Zingg Python wheel for the notebook execution path.
- Explicit single-node, dedicated-access job compute for both jobs.
- `dev` and `prod` bundle targets.

The two jobs intentionally use different Zingg execution paths. The generic job launches the Java `SparkClient` directly. The demo notebook imports the Zingg Python client, which calls the Java `zingg.spark.client.SparkClient` through the notebook Spark JVM. For that reason, the demo task installs both the Python wheel and `dist/zingg.jar`.

## Requirements

- Databricks CLI 1.3.0 or newer.
- Authentication configured for the target Databricks workspace.
- Maven and Python available on the machine performing `bundle deploy`, because bundle artifact builds happen locally before upload.
- The default `i3.xlarge` node type is AWS-specific. Override `node_type_id` for Azure, GCP, or a workspace that does not expose that node type.
- The demo notebook expects the Unity Catalog objects and paths already referenced by the notebook to exist in the target workspace.

## Deploy

```bash
databricks bundle validate -t dev
databricks bundle plan -t dev
databricks bundle deploy -t dev
```

## Run Zingg

The generic job defaults to `trainMatch` and the synchronized FEBRL configuration. Override either job parameter at run time with `--params`:

```bash
databricks bundle run -t dev \
  --params phase=trainMatch,config=/Workspace/path/to/config.json \
  zingg
```

The job maps these parameters to the Zingg command-line arguments:

```text
--phase <phase> --conf <config>
```

For production workloads, point `config` at a durable path controlled by the workspace, preferably a Unity Catalog Volume or another persistent location. Keep source data, models, labels, and outputs outside the bundle deployment directory so that destroying the bundle does not remove application state.

## Run the bundled demo notebook

```bash
databricks bundle run -t dev zingg_demo
```

The demo task installs both the Zingg wheel and JAR on its job cluster. The notebook itself is the existing `examples/databricks/FebrlExample.ipynb` from this repository and is not modified by the bundle.

The deployed notebook is a bundle-managed artifact. Treat the repository copy as the source of truth: changes made only to the deployed workspace copy are not durable and can be lost on the next deployment or on `bundle destroy`.

## Compute shape

Both jobs use ephemeral single-node job clusters with the compute fields kept explicit:

- `num_workers: 0`
- `spark.master: local[*]`
- `spark.databricks.cluster.profile: singleNode`
- `ResourceClass: SingleNode`
- `data_security_mode: DATA_SECURITY_MODE_DEDICATED`

Keeping these fields explicit also avoids relying on changing resource defaults when using the direct bundle deployment engine.

Override the node type or runtime without changing the single-node shape:

```bash
databricks bundle deploy -t dev \
  --var node_type_id=<workspace-node-type> \
  --var spark_version=16.4.x-scala2.12
```

## Checkpoint behavior

Zingg's `SparkClient` sets the Spark checkpoint directory to `/tmp/checkpoint` when the Spark context does not already have one. This supplies GraphFrames/iterative Spark operations with a writable checkpoint location on the ephemeral single-node cluster.

The checkpoint directory is intentionally run-local and disposable. Persistent Zingg state belongs in Unity Catalog Volumes or another durable customer-controlled location. If a deployment specifically requires durable checkpoints, set the Spark checkpoint directory to a writable Volume path before running Zingg rather than placing that state under the bundle deployment root.

## Remove the deployment

```bash
databricks bundle destroy -t dev
```

Destroy removes bundle-managed jobs and deployed bundle assets, including the deployed demo notebook. It is not intended to delete persistent Zingg datasets, models, labels, outputs, or other workload-created state stored outside the bundle deployment directory.
