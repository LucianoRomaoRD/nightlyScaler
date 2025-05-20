from googleapiclient.discovery import build
from google.cloud import resourcemanager_v3
from google.auth import default
from os import getenv


credentials, _ = default()
compute = build("compute", "v1", credentials=credentials)

def find_vm_across_projects(vm_name:str) -> str:
    project_ids_raw = getenv("PROJECT_IDS", None)
    if project_ids_raw is not None:
        project_ids = project_ids_raw.split(',')

    for project_id in project_ids:
        result = compute.instances().aggregatedList(project=project_id).execute()

        for _, response in result.get("items", {}).items():
            for instance in response.get("instances", []):
                if vm_name.lower() == instance['name'].lower():
                    return project_id
    
    raise RuntimeError(f"Failed to find project for vm {vm_name}")

def delete_vm(project_id: str, zone:str, vm_name: str) -> None:
    operation = compute.instances().delete(
        project=project_id,
        zone=zone,
        instance=vm_name
    ).execute()
    print(f'Execução remova da VM {vm_name}')