# Databricks Connect Guide

This guide covers how to connect Delphi to your Databricks workspace, choose between classic clusters and serverless compute, and troubleshoot version compatibility issues.

## What is Databricks Connect?

Databricks Connect lets you run PySpark code on your local machine while the actual computation happens on a remote Databricks cluster or serverless compute. Your code stays local, but the data processing runs in the cloud. Delphi uses this to execute sampling queries and metric computations against your Delta tables.

## Choosing Your Compute Target

| | Classic Cluster | Serverless Compute |
|---|---|---|
| **Setup** | Requires a running cluster + cluster ID | Just needs workspace URL |
| **Startup** | Cluster may take minutes to start | Near-instant |
| **Cost** | Billed while cluster runs | Billed per query |
| **DBR version** | You choose the runtime version | Managed by Databricks |
| **Version matching** | databricks-connect must match DBR | Always use latest |
| **Best for** | Long sessions, custom libraries | Quick checks, CI/CD |

**Recommendation:** Use serverless unless you need a specific DBR version or custom cluster configuration.

## Configuration

### Serverless (recommended)

```toml
# delphi.toml
[delphi.connection]
host = "https://your-workspace.cloud.databricks.com"
serverless = true
auth_type = "env"
# budget_policy_id = "policy-abc-123"  # optional: usage/budget policy
```

```bash
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=dapi_your_token
```

### Classic Cluster

```toml
# delphi.toml
[delphi.connection]
host = "https://your-workspace.cloud.databricks.com"
cluster_id = "0301-154530-abcdefab"
auth_type = "env"
```

### Interactive Setup

```bash
delphi setup
```

The wizard asks you to choose between classic cluster and serverless, then walks through the rest.

## Version Matching (Classic Clusters Only)

When using a classic cluster, the `databricks-connect` pip package version must be compatible with the Databricks Runtime (DBR) version on your cluster.

**The rule:** databricks-connect major.minor must be **equal to or less than** the DBR major.minor.

### How to Find Your DBR Version

1. Go to your Databricks workspace
2. Click **Compute** in the sidebar
3. Click your cluster name
4. Look for **Databricks Runtime Version** (e.g., "15.4 LTS", "16.1", "18.1")

### Installing the Right Version

Match your pip package to the DBR:

```bash
# For DBR 15.4
pip install 'databricks-connect==15.4.*'

# For DBR 16.1
pip install 'databricks-connect==16.1.*'

# For DBR 17.0
pip install 'databricks-connect==17.0.*'

# For DBR 18.1
pip install 'databricks-connect==18.1.*'
```

If you're using **uv**:

```bash
uv add 'databricks-connect==15.4.*'
```

### Serverless: No Version Matching Needed

Serverless compute is managed by Databricks. Always use the latest databricks-connect:

```bash
pip install --upgrade databricks-connect
```

## Common Errors and Fixes

### "Unsupported combination of Databricks Runtime & Databricks Connect versions"

**Cause:** Your local databricks-connect version is newer than the DBR on the cluster.

**Example:** You have `databricks-connect==18.1.2` but your cluster runs DBR 15.4.

**Fix:**
```bash
pip install 'databricks-connect==15.4.*'
```

Or upgrade your cluster to a newer DBR.

### "Can't set both cluster id and serverless"

**Cause:** Your `delphi.toml` has both `cluster_id` and `serverless = true`.

**Fix:** Use one or the other:

```toml
# Option A: cluster
[delphi.connection]
cluster_id = "0301-154530-abc"

# Option B: serverless
[delphi.connection]
serverless = true
```

### "Cluster id or serverless are required but were not specified"

**Cause:** No compute target configured.

**Fix:** Run `delphi setup` or add a connection to `delphi.toml`.

### "pyspark and databricks-connect cannot be installed at the same time"

**Cause:** Both `pyspark` and `databricks-connect` are installed. They conflict because databricks-connect bundles its own PySpark.

**Fix:**
```bash
pip uninstall -y pyspark pyspark-connect pyspark-client databricks-connect
pip install databricks-connect
```

### "Cluster ... does not exist"

**Cause:** The cluster ID in your config is wrong, or the cluster was deleted.

**Fix:** Check the cluster ID in the Databricks UI (Compute page). The format is like `0301-154530-abcdefab`.

Note: SQL warehouse IDs (like `9b07ed70821c5ba2`) are **not** cluster IDs. Databricks Connect needs a classic cluster or serverless compute, not a SQL warehouse.

### Connection timeout or cluster starting

**Cause:** The cluster is terminated and takes time to start.

**Fix:** Delphi retries automatically (3 times with exponential backoff by default). You can increase retries in `delphi.toml`:

```toml
[delphi]
connection_retries = 5
connection_timeout = 600
```

Or start the cluster manually in the Databricks UI before running tests.

## Multiple Environments

Use named profiles for different compute targets:

```toml
# Default: serverless for quick checks
[delphi.connection]
host = "https://prod.cloud.databricks.com"
serverless = true
auth_type = "env"

# Dev: specific cluster with custom libraries
[delphi.connection.profiles.dev]
host = "https://dev.cloud.databricks.com"
cluster_id = "0301-154530-dev"
auth_type = "env"

# CI: serverless for speed
[delphi.connection.profiles.ci]
host = "https://prod.cloud.databricks.com"
serverless = true
auth_type = "env"
```

Switch with `--profile`:

```bash
delphi run tests/ --profile dev
delphi run tests/ --profile ci
```

## Databricks Free Tier Limitations

Databricks free tier workspaces have restrictions:

- **No classic clusters** -- only serverless SQL warehouses are available
- **No Databricks Connect to clusters** -- you can't use cluster-based databricks-connect
- **Serverless compute may work** -- depending on workspace configuration

If you're on the free tier and can't use Databricks Connect, you can:

1. Upload Delphi as a wheel to a Unity Catalog volume
2. Install it in a notebook with `%pip install /Volumes/catalog/schema/volume/dbx_delphi-x.y.z-py3-none-any.whl`
3. Run tests directly in the notebook using the built-in `spark` session

See the [tutorial](tutorial.md#part-9-running-on-databricks) for details.
