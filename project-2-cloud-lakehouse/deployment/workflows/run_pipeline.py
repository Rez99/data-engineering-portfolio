import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request


PROJECT_ID = os.environ["PROJECT_ID"]
REGION = os.environ["REGION"]
WORKFLOW = os.environ["PIPELINE_WORKFLOW"]
BASE_URL = (
    "https://workflowexecutions.googleapis.com/v1/"
    f"projects/{PROJECT_ID}/locations/{REGION}/workflows/{WORKFLOW}/executions"
)

PHASES = {
    "initialize": "Initializing workflow",
    "create_cluster": "Creating Spark cluster",
    "get_create_operation": "Creating Spark cluster",
    "check_create_operation": "Creating Spark cluster",
    "wait_for_cluster": "Creating Spark cluster",
    "submit_extract": "Extracting clickstream sample",
    "wait_for_extract": "Extracting clickstream sample",
    "check_extract": "Extracting clickstream sample",
    "wait_before_extract_poll": "Extracting clickstream sample",
    "extract_complete": "Extracting clickstream sample",
    "submit_load": "Loading CSV into Iceberg",
    "wait_for_load": "Loading CSV into Iceberg",
    "check_load": "Loading CSV into Iceberg",
    "wait_before_load_poll": "Loading CSV into Iceberg",
    "load_complete": "Loading CSV into Iceberg",
    "submit_transform": "Building dbt feature table",
    "wait_for_dbt": "Building dbt feature table",
    "check_dbt": "Building dbt feature table",
    "wait_before_dbt_poll": "Building dbt feature table",
    "dbt_complete": "Building dbt feature table",
    "submit_train": "Training XGBoost model",
    "wait_for_train": "Training XGBoost model",
    "check_train": "Training XGBoost model",
    "wait_before_train_poll": "Training XGBoost model",
    "train_complete": "Training XGBoost model",
    "delete_cluster": "Deleting Spark cluster",
    "get_delete_operation": "Deleting Spark cluster",
    "check_delete_operation": "Deleting Spark cluster",
    "wait_for_delete": "Deleting Spark cluster",
    "finish_spark": "Finalizing Spark work",
    "complete": "Completing workflow",
}


def get_access_token() -> str:
    return subprocess.check_output(
        ["gcloud", "auth", "print-access-token"],
        text=True,
    ).strip()


def request(method: str, url: str, access_token: str, body=None):
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        sys.stderr.write(error.read().decode("utf-8") + "\n")
        raise


def current_step(execution) -> str | None:
    steps = execution.get("status", {}).get("currentSteps", [])
    if not steps:
        return None
    return steps[0].get("step")


def print_result(execution) -> None:
    result = execution.get("result")
    if result:
        print(result, flush=True)


def main() -> None:
    access_token = get_access_token()

    print(f"🟢 Workflow: starting {WORKFLOW}", flush=True)
    execution = request("POST", BASE_URL, access_token, {})
    execution_name = execution["name"]
    execution_id = execution_name.rsplit("/", 1)[-1]
    print(f"   - Execution: {execution_id}", flush=True)

    last_phase = None
    while True:
        execution = request(
            "GET",
            f"https://workflowexecutions.googleapis.com/v1/{execution_name}",
            access_token,
        )
        state = execution.get("state")
        step = current_step(execution)
        phase = PHASES.get(step, step or state)

        if phase and phase != last_phase and state == "ACTIVE":
            print(f"🟡 {phase}", flush=True)
            last_phase = phase

        if state == "SUCCEEDED":
            print("🟢 Pipeline complete", flush=True)
            print_result(execution)
            return

        if state in {"FAILED", "CANCELLED"}:
            print(f"🔴 Pipeline {state.lower()}", file=sys.stderr, flush=True)
            if execution.get("error"):
                print(
                    json.dumps(execution["error"], indent=2),
                    file=sys.stderr,
                    flush=True,
                )
            sys.exit(1)

        time.sleep(15)


if __name__ == "__main__":
    main()
