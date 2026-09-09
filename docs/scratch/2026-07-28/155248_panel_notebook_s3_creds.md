# Panel + notebooks S3 creds → RustFS admin / cyberphy

## Root cause
Live 1.6.5 deploy still had:
- AWS_ACCESS_KEY_ID=minioadmin (no longer valid on RustFS)
- S3_BUCKET=cybersec-dask-data (bucket does not exist on RustFS)

RustFS accepts admin/admin; buckets cyberphy + cyberphy-hx.

## Source updates
- zarf/notebooks/{OTEL_Data_Generator,Dask_S3_Validation}.ipynb — default bucket cyberphy; s3fs path-style + env keys
- zarf/images/sample-notebooks/* mirrored
- zarf/manifests/sample-notebooks-configmap.yaml regenerated
- Comments: panel-viz, vpc-flow, jupyterhub-values, otel-navigator (MinIO→RustFS)

## Live patch (no full zarf redeploy)
- secret panel-viz/otel-navigator-credentials → admin/admin + endpoint
- cm otel-navigator-config + vpc-flow-generator-config → cyberphy
- daskcluster cybersec-dask env → admin/admin
- jupyterhub hub secret values.yaml singleuser.extraEnv → admin/admin/cyberphy
- sample-notebooks ConfigMap reapplied
- rollouts restarted

## Operator note
Next `zarf:local:deploy` must pass:
  --set S3_BUCKET=cyberphy --set S3_ACCESS_KEY=admin --set S3_SECRET_KEY=admin
(lab_env / devenv task already defaults these.)
