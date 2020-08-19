#!/usr/bin/env python3
# verify there are no miniwdl-related docker services left behind from test suite
import sys
import docker

client = docker.from_env(version="auto")
info = client.info()
if (
    "Swarm" in info
    and "LocalNodeState" in info["Swarm"]
    and info["Swarm"]["LocalNodeState"] != "inactive"
):
    miniwdl_services = [
        d
        for d in [s.attrs for s in client.services.list()]
        if "Spec" in d and "Labels" in d["Spec"] and "miniwdl_run_id" in d["Spec"]["Labels"]
    ]
    if miniwdl_services:
        for d in miniwdl_services:
            print(d["Spec"]["Labels"]["miniwdl_run_id"])
        print("Docker swarm lists existing miniwdl-related services, suggesting miniwdl task runtime failed to clean up after itself. Debug this and/or `docker swarm leave --force` to clear.", file=sys.stderr)
        sys.exit(1)
