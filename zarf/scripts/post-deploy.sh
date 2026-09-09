#!/bin/bash
# Post-deployment verification script for Cyberphy Dask package
#
# This script runs after Zarf deploys all components to verify
# the deployment is working correctly.
#
# Usage: ./post-deploy.sh

set -e

echo "=== Cyberphy Dask Post-Deployment Verification ==="
echo ""

# Check namespaces
echo "Checking namespaces..."
kubectl get ns dask panel-viz jupyterhub 2>/dev/null || {
    echo "Warning: Some namespaces not found"
}

# Check Dask Operator
echo ""
echo "Checking Dask Operator..."
kubectl get pods -n dask-operator -l app.kubernetes.io/name=dask-kubernetes-operator 2>/dev/null || {
    echo "Warning: Dask Operator not found"
}

# Check DaskCluster
echo ""
echo "Checking DaskCluster..."
kubectl get daskcluster -n dask 2>/dev/null || {
    echo "Warning: No DaskCluster found"
}

# Check Dask pods
echo ""
echo "Checking Dask pods..."
kubectl get pods -n dask -l dask.org/cluster-name=cybersec-dask 2>/dev/null || {
    echo "Warning: No Dask pods found"
}

# Check JupyterHub (if deployed)
echo ""
echo "Checking JupyterHub..."
kubectl get pods -n jupyterhub 2>/dev/null || {
    echo "Info: JupyterHub not deployed or namespace not found"
}

# Check Panel-Viz
echo ""
echo "Checking Panel-Viz..."
kubectl get pods -n panel-viz -l app=panel-viz 2>/dev/null || {
    echo "Warning: Panel-Viz not found"
}

# Check services and NodePorts
echo ""
echo "=== Service Endpoints ==="
echo ""
echo "Dask Dashboard:"
echo "  NodePort: kubectl get svc -n dask cybersec-dask-scheduler -o jsonpath='{.spec.ports[?(@.name==\"tcp-dashboard\")].nodePort}'"
echo "  -> Access: http://<node-ip>:30087"
echo ""
echo "Panel-Viz:"
echo "  NodePort: 30506"
echo "  -> Access: http://<node-ip>:30506"
echo ""
echo "JupyterHub:"
echo "  NodePort: 30080"
echo "  -> Access: http://<node-ip>:30080"
echo ""

# Check ingress (if deployed)
echo "=== Ingress Resources ==="
kubectl get ingress -A 2>/dev/null || {
    echo "Info: No ingress resources found (NodePort access available)"
}

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "Access services via NodePort or configure ingress hostnames in /etc/hosts:"
echo "  <node-ip> dask.cybersec.local jupyter.cybersec.local panel.cybersec.local"
