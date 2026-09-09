"""World interface for the convergence engine.

How the engine talks to the cluster (kubectl) and the bundle (zarf), plus the run
context. kubectl-only at runtime so the engine runs node-side, operator-side, or as
an in-cluster Job. ``zarf`` is only needed for the EXPENSIVE init/push remediations;
if it's absent those degrade to a precise manual hint rather than failing.

Image presence (a Layer-A / CLOSURE concern) is checked via POD HEALTH
(ImagePullBackOff ⇒ image missing) rather than node-local ``crictl`` — this keeps
the check cross-node and runnable from anywhere with a kubeconfig.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, List, Optional

DEFAULT_MANIFEST = Path(__file__).resolve().parent.parent / "artifacts.manifest.json"
# Bundled k8s manifests (local-path-provisioner.yaml) live next to the package source
# locally; converge-aws.sh stages them into <stage>/manifests/ to match this default.
DEFAULT_MANIFESTS_DIR = Path(__file__).resolve().parent.parent / "manifests"


@dataclass
class Ctx:
    kubectl: List[str]                      # base argv, e.g. ["zarf","tools","kubectl"]
    apply: bool = False                     # actually remediate (vs dry-run/verify)
    manifest: dict = field(default_factory=dict)
    zarf_bin: Optional[str] = None
    package_path: Optional[str] = None
    manifests_dir: Optional[str] = None      # dir holding bundled manifests (local-path-provisioner.yaml)
    registry_pv_size: str = "5Gi"
    registry_pvc_enabled: bool = True
    # DEFAULT (resilient air-gap, the only modality public releases target): the Zarf
    # registry binds a claimRef hostPath PV, so NO default StorageClass / local-path-
    # provisioner / bootstrap images are needed. Set True only to opt a resourced
    # multi-node cluster back into dynamic provisioning for workloads that require it.
    dynamic_provisioning: bool = False
    s3: dict = field(default_factory=dict)
    timeout: int = 90
    verbose: bool = False
    _cap_cache: Optional[dict] = None
    _zarf_path: Optional[str] = None        # cached PATH guaranteeing kubectl (see zarf())

    # ------------------------------------------------------------------ process
    @staticmethod
    def out_text(val) -> str:
        """Normalize subprocess stdout/stderr to str (TimeoutExpired may leave bytes)."""
        if val is None:
            return ""
        if isinstance(val, bytes):
            return val.decode("utf-8", errors="replace")
        return str(val)

    def run(self, argv: List[str], timeout: Optional[int] = None,
            input_: Optional[str] = None,
            env: Optional[dict] = None) -> subprocess.CompletedProcess:
        if self.verbose:
            print("    $ " + " ".join(argv))
        # ``env`` is MERGED onto the inherited environment — use it to pass secrets
        # (e.g. ZARF_VAR_S3_SECRET_KEY) so they never land on argv / the process table.
        run_env = {**os.environ, **env} if env else None
        try:
            return subprocess.run(
                argv, capture_output=True, text=True,
                timeout=timeout or self.timeout, input=input_, env=run_env,
            )
        except FileNotFoundError as e:
            return subprocess.CompletedProcess(argv, 127, "", str(e))
        except subprocess.TimeoutExpired as e:
            # e.stdout/stderr can be bytes even when text=True was requested
            return subprocess.CompletedProcess(
                argv, 124, self.out_text(e.stdout),
                self.out_text(e.stderr) or "timeout")

    def run_stream(
        self,
        argv: List[str],
        timeout: Optional[int] = None,
        env: Optional[dict] = None,
        abort_check: Optional[Callable[[], Optional[str]]] = None,
        abort_every: float = 10.0,
        prefix: str = "    | ",
    ) -> subprocess.CompletedProcess:
        """Run a long command with live stdout (line-buffered) + optional abort.

        Used for ``zarf package deploy`` so operators see progress and the engine
        can kill the child on terminal cluster failure (ImagePullBackOff, etc.)
        instead of waiting the full wall-clock timeout (converge-24: 7200s silent).
        """
        if self.verbose:
            print("    $ " + " ".join(argv), flush=True)
        run_env = {**os.environ, **env} if env else None
        wall = timeout or self.timeout
        try:
            proc = subprocess.Popen(
                argv,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=run_env,
            )
        except FileNotFoundError as e:
            return subprocess.CompletedProcess(argv, 127, "", str(e))

        lines: List[str] = []
        deadline = time.monotonic() + wall
        next_abort = time.monotonic() + abort_every
        abort_reason: Optional[str] = None
        try:
            import select
            assert proc.stdout is not None
            fd = proc.stdout.fileno()
            while True:
                now = time.monotonic()
                if now >= deadline:
                    abort_reason = f"timeout after {wall}s"
                    break
                if abort_check and now >= next_abort:
                    next_abort = now + abort_every
                    try:
                        why = abort_check()
                    except Exception as e:  # never kill deploy on census bugs
                        why = None
                        if self.verbose:
                            print(f"    (abort_check error ignored: {e})", flush=True)
                    if why:
                        abort_reason = why
                        break
                if proc.poll() is not None:
                    rest = proc.stdout.read() or ""
                    for ln in rest.splitlines():
                        print(prefix + ln, flush=True)
                        lines.append(ln + "\n")
                    break
                # select so abort/timeout run even when zarf is silent for minutes
                ready, _, _ = select.select([fd], [], [], 1.0)
                if not ready:
                    continue
                line = proc.stdout.readline()
                if line:
                    print(prefix + line.rstrip("\n"), flush=True)
                    lines.append(line)
                    if len(lines) > 400:
                        lines = lines[-250:]
        finally:
            if proc.poll() is None:
                try:
                    proc.terminate()
                    try:
                        proc.wait(timeout=8)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=5)
                except OSError:
                    pass

        out = "".join(lines)
        if abort_reason:
            return subprocess.CompletedProcess(
                argv, 125, out, f"aborted: {abort_reason}")
        rc = proc.returncode if proc.returncode is not None else 1
        return subprocess.CompletedProcess(argv, rc, out, "")

    def k(self, args: List[str], timeout: Optional[int] = None) -> subprocess.CompletedProcess:
        return self.run(self.kubectl + args, timeout=timeout)

    def kjson(self, args: List[str]) -> Any:
        r = self.k(args + ["-o", "json"])
        if r.returncode != 0 or not r.stdout.strip():
            return None
        try:
            return json.loads(r.stdout)
        except json.JSONDecodeError:
            return None

    def get(self, kind: str, name: str = "", ns: Optional[str] = None) -> Optional[dict]:
        args = ["get", kind]
        if name:
            args.append(name)
        if ns:
            args += ["-n", ns]
        elif ns is None and kind not in ("nodes", "node", "ns", "namespace",
                                         "pv", "storageclass", "sc", "crd"):
            args.append("-A")
        obj = self.kjson(args)
        if obj is None:
            return None
        # single-name get returns the object; otherwise a List
        return obj

    def items(self, kind: str, ns: Optional[str] = None,
              selector: Optional[str] = None) -> List[dict]:
        args = ["get", kind]
        if ns:
            args += ["-n", ns]
        else:
            args.append("-A")
        if selector:
            args += ["-l", selector]
        obj = self.kjson(args)
        if not obj:
            return []
        return obj.get("items", []) if isinstance(obj, dict) else []

    def exists(self, kind: str, name: str, ns: Optional[str] = None) -> bool:
        args = ["get", kind, name]
        if ns:
            args += ["-n", ns]
        return self.k(args).returncode == 0

    def apply_yaml(self, yaml_text: str) -> subprocess.CompletedProcess:
        return self.run(self.kubectl + ["apply", "-f", "-"], input_=yaml_text)

    # ------------------------------------------------------------------ zarf
    def have_zarf(self) -> bool:
        return bool(self.zarf_bin) and (
            Path(self.zarf_bin).exists() or shutil.which(self.zarf_bin) is not None
        )

    def _zarf_env(self, env: Optional[dict] = None) -> dict:
        # Zarf component actions run bare `kubectl` in a plain shell — but air-gap
        # RKE2 nodes keep kubectl at /var/lib/rancher/rke2/bin, off root's PATH
        # (field 2026-07-15: every dask-cluster after-action died `kubectl: command
        # not found`, and the required-component rider blocked ALL deploys). Hand
        # the zarf subprocess a PATH guaranteed to resolve kubectl, and a
        # KUBECONFIG if the environment lacks one.
        e = dict(env or {})
        e.setdefault("PATH", self._path_with_kubectl())
        if not os.environ.get("KUBECONFIG"):
            rke2_kc = "/etc/rancher/rke2/rke2.yaml"
            if os.access(rke2_kc, os.R_OK):
                e.setdefault("KUBECONFIG", rke2_kc)
        return e

    def zarf(self, args: List[str], timeout: int = 3600,
             env: Optional[dict] = None) -> subprocess.CompletedProcess:
        # Default timeout 3600s: ``package deploy --components=X`` still pulls
        # required riders; air-gap image push + helm can exceed 30m. Prefer
        # ``zarf_stream`` for long deploys so progress is visible.
        return self.run(
            [str(self.zarf_bin)] + args, timeout=timeout, env=self._zarf_env(env))

    def zarf_stream(
        self,
        args: List[str],
        timeout: int = 3600,
        env: Optional[dict] = None,
        abort_check: Optional[Callable[[], Optional[str]]] = None,
        abort_every: float = 10.0,
    ) -> subprocess.CompletedProcess:
        """Like ``zarf()`` but streams stdout live and supports early abort."""
        return self.run_stream(
            [str(self.zarf_bin)] + args,
            timeout=timeout,
            env=self._zarf_env(env),
            abort_check=abort_check,
            abort_every=abort_every,
        )

    def _path_with_kubectl(self) -> str:
        """PATH for zarf subprocesses that is guaranteed to resolve `kubectl`:
        the inherited PATH, plus known node locations, plus — if kubectl is
        genuinely absent — a shim directory whose `kubectl` delegates to
        `zarf tools kubectl` (always available: we ship the binary)."""
        if self._zarf_path:
            return self._zarf_path
        path = os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin")
        parts = path.split(os.pathsep)
        for d in ("/var/lib/rancher/rke2/bin", "/var/lib/rancher/k3s/bin"):
            if d not in parts and Path(d, "kubectl").exists():
                path = path + os.pathsep + d
        if not shutil.which("kubectl", path=path) and self.zarf_bin:
            zarf_abs = shutil.which(str(self.zarf_bin)) or str(Path(self.zarf_bin).resolve())
            import tempfile
            shim_dir = tempfile.mkdtemp(prefix="converge-kubectl-shim-")
            shim = Path(shim_dir, "kubectl")
            shim.write_text(f'#!/bin/sh\nexec "{zarf_abs}" tools kubectl "$@"\n')
            shim.chmod(0o755)
            path = shim_dir + os.pathsep + path
        self._zarf_path = path
        return path

    # ------------------------------------------------------------------ derived
    def pod_image_missing(self, ns: str, selector: str) -> Optional[bool]:
        """True if any matching pod is stuck pulling its image (image absent in the
        closed world); False if pods exist and none are; None if no pods to judge."""
        pods = self.items("pods", ns=ns, selector=selector)
        if not pods:
            return None
        bad = ("ImagePullBackOff", "ErrImagePull", "ErrImageNeverPull")
        for p in pods:
            for cs in (p.get("status", {}).get("containerStatuses", []) +
                       p.get("status", {}).get("initContainerStatuses", [])):
                waiting = (cs.get("state", {}) or {}).get("waiting") or {}
                if waiting.get("reason") in bad:
                    return True
        return False

    def pods_ready(self, ns: str, selector: str) -> tuple:
        """(ready_count, total) for Ready pods matching selector."""
        pods = self.items("pods", ns=ns, selector=selector)
        ready = 0
        for p in pods:
            conds = {c["type"]: c["status"] for c in p.get("status", {}).get("conditions", [])}
            if conds.get("Ready") == "True":
                ready += 1
        return ready, len(pods)

    def node_capacity(self) -> dict:
        """Rough schedulable capacity across Ready, non-tainted-NoSchedule nodes.
        Used to size workers to capacity (the worker-replica cap) and for the banner.
        There is NO topology branch — the resilient path runs identically on one node
        or many; capacity only sizes the worker count, it never forks behavior."""
        if self._cap_cache is not None:
            return self._cap_cache
        nodes = self.items("nodes")
        ready = 0
        total_mem_gib = 0.0
        worker_like = 0
        for n in nodes:
            conds = {c["type"]: c["status"] for c in n.get("status", {}).get("conditions", [])}
            if conds.get("Ready") != "True":
                continue
            taints = n.get("spec", {}).get("taints", []) or []
            blocked = any(t.get("effect") in ("NoSchedule", "NoExecute") and
                          "control-plane" not in t.get("key", "") for t in taints)
            ready += 1
            mem = n.get("status", {}).get("allocatable", {}).get("memory", "0")
            total_mem_gib += _mem_to_gib(mem)
            if not blocked:
                worker_like += 1
        cap = {"ready_nodes": ready, "schedulable_nodes": max(worker_like, 1),
               "total_mem_gib": round(total_mem_gib, 1)}
        self._cap_cache = cap
        return cap


def _mem_to_gib(v: str) -> float:
    v = v.strip()
    units = {"Ki": 1 / 2**20, "Mi": 1 / 2**10, "Gi": 1.0, "Ti": 1024.0,
             "K": 1e3 / 2**30, "M": 1e6 / 2**30, "G": 1e9 / 2**30}
    for u, mul in units.items():
        if v.endswith(u):
            try:
                return float(v[:-len(u)]) * mul
            except ValueError:
                return 0.0
    try:
        return float(v) / 2**30
    except ValueError:
        return 0.0


def load_manifest(path: Optional[str] = None) -> dict:
    p = Path(path) if path else DEFAULT_MANIFEST
    return json.loads(p.read_text())


def bootstrap_image_refs(manifest: dict) -> List[str]:
    return [i["ref"] for i in manifest.get("bootstrap_images", {}).get("images", [])]
