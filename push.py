import os

import nextmv
from nextmv.cloud import Application, Client

from src import APP_ID, INSTANCE_ID, INSTANCE_NAME, MANIFEST

api_key = os.environ.get("NEXTMV_API_KEY")
if api_key is None:
    raise Exception("Please set NEXTMV_API_KEY environment variable")

version_id = os.environ.get("VERSION_ID")
if version_id is None:
    raise Exception("Please set VERSION_ID environment variable")

client = Client(api_key=api_key)
if Application.exists(client, id=APP_ID):
    app = Application(client=client, id=APP_ID)
else:
    app = Application.new(client=client, id=APP_ID, name=APP_ID)


manifest = nextmv.cloud.Manifest(
    type=nextmv.cloud.ManifestType.PYTHON,
    runtime=nextmv.cloud.ManifestRuntime.PYTHON,
    files=MANIFEST,
    python=nextmv.cloud.ManifestPython(pip_requirements="src/model/requirements.txt"),
)

app_dir = os.path.dirname(os.path.abspath(__file__))
app.push(app_dir=app_dir, manifest=manifest, verbose=True)
app.new_version(id=version_id, name=version_id)
if app.instance_exists(INSTANCE_ID):
    app.update_instance(version_id=version_id, id=INSTANCE_ID, name=INSTANCE_NAME)
else:
    app.new_instance(version_id=version_id, id=INSTANCE_ID, name=INSTANCE_NAME)
