# k8s — Kustomize manifests for Vector services

## Structure

```
base/<service>/          # env-agnostic manifests
  deployment.yaml        # resource sizes match ECS task (CPU/memory)
  serviceaccount.yaml    # no AWS annotations — IAM handled by Pod Identity (SEN-230)
  kustomization.yaml
overlays/
  prod/                  # namespace: vector, ECR: 030797368798
  staging/               # namespace: vector, ECR: 579039992622
```

## Services

| Service | CPU | Memory |
|---|---|---|
| watcher | 2000m | 4096Mi |
| backup-worker | 500m | 1024Mi |
| oracle-price-worker | 1000m | 2048Mi |
| sparklend-position-tracker | 1000m | 2048Mi |
| morpho-indexer | 1000m | 2048Mi |
| allocation-tracker | 1000m | 2048Mi |
| avalanche-sparklend-position-tracker | 1000m | 2048Mi |

## How overlays work

Each overlay sets the target namespace and pins ECR image names. CI bumps `newTag` per service on merge to main.

```bash
# Preview what gets applied
kustomize build k8s/overlays/prod | kubectl diff -f -
```

## IAM / AWS credentials

Service accounts have no IRSA annotations. AWS access is wired via **EKS Pod Identity** associations in the infra repo (SEN-230) — no changes needed here.

## Namespaces

`vector` is managed by ArgoCD in the infra gitops repo. Do not add namespace manifests here.
