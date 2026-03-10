# Repository Agent Notes

## Kubernetes Deployment Workflow

- Always use `./helm/start.sh` to deploy to Kubernetes.
- Always use `./helm/stop.sh` to tear down Kubernetes deployments.
- Do not use direct `helm upgrade/install/uninstall` for normal workflow unless explicitly requested.
