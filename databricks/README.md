# Zingg Databricks bundle

This directory contains the Databricks Declarative Automation Bundle resources for deploying Zingg to Databricks.

The bundle uses the direct deployment engine and creates only ephemeral single-node job compute. It does not create a persistent interactive cluster.

## What is included

- `zingg`: generic Spark JAR job that launches `zingg.spark.client.SparkClient` and accepts Zingg `--phase` and `--conf` arguments.
- `zingg_demo`: job that runs the repository's existing `examples/databricks/FebrlExample.ipynb` notebook.
- Java artifact build and upload for the Zingg assembly JAR.
- Python wheel build and upload for the demo notebook.
- A Databricks single-node job cluster definition with zero workers.
- `dev` and `prod` bundle targets.

## Requirements

- Databricks CLI 1.3.0 or newer.
- Authentication configured for the target Databricks workspace.
- Maven and Python available on the machine performing `bundle deploy`, because bundle artifact builds happen locally before upload.
- The default `i3.xlarge` node type is AWS-specific. Override `node_type_id` for Azure, GCP, or a workspace that does not expose that node type.

## Deploy

```bash
databricks bundle validate -t dev
databricks bundle plan -t dev
databricks bundle deploy -t dev
```

## Run Zingg

The generic job defaults to `trainMatch` and the synchronized FEBRL configuration. Override either parameter at run time:

```bash
databricks bundle run -t dev zingg -- --phase=trainMatch --config=/Workspace/path/to/config.json
```

The job maps these parameters to:

```text
--phase <phase> --conf <config>
```

For production workloads, point `config` at a durable path controlled by the workspace, preferably a Unity Catalog Volume or another persistent location. Keep data, models, labels, and outputs outside the bundle deployment directory so that destroying the bundle does not remove application state.

## Run the bundled demo notebook

```bash
databricks bundle run -t dev zingg_demo
```

The demo job deploys and runs the existing Zingg `examples/databricks/FebrlExample.ipynb` notebook. The notebook expects the Unity Catalog objects and paths referenced by that notebook to exist in the target workspace.

## Override compute

```bash
databricks bundle deploy -t dev \
  --var node_type_id=<workspace-node-type> \
  --var spark_version=16.4.x-scala2.12
```

The job compute remains single-node because the resource sets `num_workers: 0`, the Databricks single-node cluster profile, and `ResourceClass=SingleNode`.

## Remove the deployment

```bash
databricks bundle destroy -t dev
```

Destroy removes the bundle-managed jobs and deployed bundle assets. It is not intended to delete persistent Zingg datasets, models, labels, or output stored elsewhere.
