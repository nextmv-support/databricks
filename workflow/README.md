# Generic databricks job runner

This app aims to run arbitrary databricks jobs via the Nextmv Platform.

## Setup

1. Setup databricks side:
    - Create job on databricks side.
    - Note its ID and set it as a parameter (`db_job_id`) of the app (typically on the instance).
    - Set further parameters that your job requires via the app options (e.g., `solve_duration=60`). The options will get passed to the job as parameters.
    - Make sure the job's tasks emit json based results via `dbutils.notebook.exit(json.dumps(output))`. Unfortunately, DBX only supports it for notebooks. For Python scripts, you can use a subsequent notebook task to forward the script's output (e.g., use `dbutils.jobs.taskValues.set(key="result", value=json.dumps(output))` in script task to set and `dbutils.jobs.taskValues.get(taskKey="script-task", key="result", debugValue='{"data":"missing"}')` in notebook task to retrieve it - and then emit via notebook exit like usual).
2. Create a [secret collection][secret] in the [Nextmv Console][console] with
   the name `databricks` and add the following entries:
    - `DATABRICKS_HOST`: The host URL of your Databricks instance (e.g., `https://adb-1234567890123456.7.azuredatabricks.net`).
    - `DATABRICKS_TOKEN`: A personal access token for your Databricks instance

## Run the app

You can run the app using the Nextmv CLI. Make sure you have it installed by
following the [installation guide][install-cli]. We don't need to pass an input,
as the app will run a Databricks job that does not require any input data from
this side.

```bash
echo '{}' |  nextmv app run -a test -s dbx -o 'db_job_id=1008475154783045'
```

## Next steps

- Visit our [docs][docs] and [blog][blog]. Need more assistance?
  [Contact][contact] us!

[docs]: https://docs.nextmv.io
[console]: https://cloud.nextmv.io
[secret]: https://www.nextmv.io/docs/using-nextmv/reference/secret-collections
[install-cli]: https://docs.nextmv.io/docs/using-nextmv/setup/install#nextmv-cli
[blog]: https://www.nextmv.io/blog
[contact]: https://www.nextmv.io/contact
