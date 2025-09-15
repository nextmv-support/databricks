import json
import os
import sys
import time

import nextmv
from databricks.sdk import WorkspaceClient
from nextpipe import FlowSpec, log, needs, step


# >>> Workflow definition
class Flow(FlowSpec):
    @step
    def dbx(data: dict):
        """Run DBX job."""
        # We pass the input data as a `data` parameter to the job, if available and
        # json-serializable.
        input_data: dict = None
        if data:
            try:
                json.dumps(data)
                input_data = data
            except Exception as err:
                log(f"Input data is not JSON serializable, cannot pass to DBX job: {err}")

        # Parse options from command line arguments.
        options = parse_options()
        log(f"Databricks job ID: {DB_JOB}")
        log(f"Parsed options {len(options)}:")
        for key, value in options.items():
            log(f"  {key} = {value}")

        # Set input data if available.
        if input_data is not None:
            options["data"] = json.dumps(input_data)
            log(f"Passing input data to DBX job, {len(options['data'])} bytes.")

        # Run the Databricks job with the specified job ID and options.
        log("Starting Databricks job...")
        solution, stats = run_databricks_job(DB_JOB, options)
        log("Databricks job completed successfully.")

        return nextmv.Output(
            output_format=nextmv.OutputFormat.JSON,
            options=options | {"db_job_id": DB_JOB},
            solution=solution,
            statistics=stats,
        )

    @needs(predecessors=[dbx])
    @step
    def enhance(solution_output: nextmv.Output):
        """Enhances the result."""
        log("Adding custom plot...")

        # As the user may change the tasks around, we search for the workforce scheduling
        # results we are aiming to extend with a plot.
        solution: dict | None = None
        for task_output in solution_output.solution.values():
            if isinstance(task_output, dict) and "shiftsPerWorker" in task_output:
                solution = task_output
                break
        if solution is None:
            log("No solution data found from DBX job, skipping enhancement.")
            return solution_output

        # Create a simple bar chart of shifts assigned to each worker.
        shifts_per_worker = solution["shiftsPerWorker"]
        workers = list(shifts_per_worker.keys())
        shifts_counts = [shifts_per_worker[w] for w in workers]
        plot = {
            "data": [{"x": workers, "y": shifts_counts, "type": "bar", "name": "Shifts"}],
            "layout": {
                "title": {"text": "Shift Assignment Distribution"},
                "xaxis": {"title": {"text": "Name"}},
                "yaxis": {"title": {"text": "Number of Shifts"}},
            },
        }

        # Attach the plot as an asset to the solution output.
        solution_output.assets = [
            nextmv.Asset(
                name="assignments",
                content=plot,
                description="Bar chart of shifts assigned to each worker",
                visual=nextmv.Visual(visual_schema=nextmv.VisualSchema.PLOTLY, label="Shift Assignments"),
            )
        ]
        return solution_output


def main():
    # Read input.
    input_data = nextmv.load()

    # Run workflow
    flow = Flow("DecisionFlow", input_data.data)
    flow.run()

    # Write out the result
    nextmv.write(flow.get_result(flow.enhance))


# DBX AUXILIARY
# This functionality is still a bit experimental, but may eventually move to be supported
# directly in nextmv.

DB_API_KEY = os.getenv("DATABRICKS_TOKEN", "")
if not DB_API_KEY:
    raise ValueError("DATABRICKS_TOKEN environment variable is not set.")
DB_HOST = os.getenv("DATABRICKS_HOST", "")
if not DB_HOST:
    raise ValueError("DATABRICKS_HOST environment variable is not set.")
DB_JOB = os.getenv("DATABRICKS_JOB_ID", "")
if not DB_JOB:
    raise ValueError("DATABRICKS_JOB_ID environment variable is not set.")


def parse_options() -> dict[str, str]:
    """
    Parses all arguments passed and prepares them as a key-value dictionary so that they
    can be passed as parameters to the databricks job.
    """
    options = {}
    for arg in sys.argv[1:]:
        if arg.startswith("--"):
            arg = arg[2:]
        elif arg.startswith("-"):
            arg = arg[1:]
        if "=" in arg:
            key, value = arg.split("=", 1)
            options[key] = value
        else:
            options[arg] = "true"
    return options


def read_input() -> str:
    """
    Reads input from stdin and returns the string.
    """
    input_data = sys.stdin.read() if not sys.stdin.isatty() else ""
    if not input_data:
        return ""
    return input_data.strip()


def run_databricks_job(job_id: str, parameters: dict[str, str]) -> tuple[dict[str, str], nextmv.Statistics]:
    """
    Runs a Databricks job with the given job ID and parameters.
    Returns the output of the job run.
    """
    # Authenticate (assumes DATABRICKS_HOST and DATABRICKS_TOKEN env vars are set as Nextmv secrets)
    w = WorkspaceClient(host=DB_HOST, token=DB_API_KEY)

    # Make sure the job exists
    try:
        w.jobs.get(job_id)
    except Exception as err:
        nextmv.log(f"Databricks job with ID {job_id} not found")
        raise Exception(f"Databricks job with ID {job_id} not found") from err

    # Run the job
    before = time.time()
    run = w.jobs.run_now(
        job_id=job_id,
        job_parameters=parameters,
    ).result()
    duration = time.time() - before

    # Log basics about run execution
    nextmv.log(f"Run ID: {run.run_id}")
    nextmv.log(f"Job ID: {job_id}")
    nextmv.log(f"Execution state: {run.status.state}")
    nextmv.log(f"Execution duration: {(run.execution_duration or 0) / 1000} seconds")

    # Collect statistics about the run
    statistics = nextmv.Statistics(
        run=nextmv.RunStatistics(
            custom={
                "run_id": run.run_id,
                "job_id": job_id,
                "state": run.status.state,
                "duration": duration,
            }
        )
    )

    # Collect results of all tasks
    if not run.tasks:
        nextmv.log("No tasks found in the run.")
        return {}
    results = {}
    for task in run.tasks:
        run_id = task.run_id
        run_output = w.jobs.get_run_output(run_id=run_id)
        nextmv.log(f"Task ID: {task.task_key}, Run ID: {run_id}")
        nextmv.log(f"Task state: {task.state}")
        nextmv.log(f"Task logs:\n{run_output.logs}")
        task_output = {}

        # Collect notebook output if available
        if (
            hasattr(run_output, "notebook_output")
            and hasattr(run_output.notebook_output, "result")
            and run_output.notebook_output.result is not None
        ):
            if isinstance(run_output.notebook_output.result, dict):
                task_output = run_output.notebook_output.result
            elif isinstance(run_output.notebook_output.result, str):
                # Try to parse as JSON if it's a string
                try:
                    task_output = json.loads(run_output.notebook_output.result)
                except json.JSONDecodeError:
                    nextmv.log("Failed to parse notebook output as JSON, using as string")
                    task_output = run_output.notebook_output.result
            else:
                nextmv.log(f"Unexpected notebook output type, ignoring: {type(run_output.notebook_output.result)}")
        else:
            nextmv.log(f"No notebook output found for task {task.task_key} (Run ID: {run_id})")

        results[task.task_key] = task_output

    statistics.result = nextmv.ResultStatistics(custom=results)

    return results, statistics


if __name__ == "__main__":
    main()
