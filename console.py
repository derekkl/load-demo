"""Load-demo console: web UI + API for driving the worker teams and watching scheduling.

- Serves desired load to workers (/api/desired), persisted in a ConfigMap
- Reads nodes, pods, deployments, metrics and events from the Kubernetes API
- Scales team deployments, cordons/uncordons nodes, drains demo pods via the
  Eviction API (so PodDisruptionBudgets are honoured, like `oc adm drain`)
Python stdlib only.
"""
import json
import os
import ssl
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SA_DIR = os.environ.get("SA_DIR", "/var/run/secrets/kubernetes.io/serviceaccount")
API = os.environ.get("KUBE_API", "https://kubernetes.default.svc")
SELF_NS = os.environ.get("POD_NAMESPACE", "load-console")
STATE_CM = os.environ.get("STATE_CONFIGMAP", "load-console-state")
TEAM_NS = [n.strip() for n in os.environ.get("TEAM_NAMESPACES", "team-guarded,team-wild").split(",") if n.strip()]
SEL = "app.kubernetes.io/part-of=load-demo"
MAX_CPU_M = int(os.environ.get("MAX_CPU_M", "4000"))
MAX_MEM_MB = int(os.environ.get("MAX_MEM_MB", "6144"))
MAX_REPLICAS = int(os.environ.get("MAX_REPLICAS", "60"))
REFRESH = float(os.environ.get("REFRESH_SECONDS", "3"))
STATIC = os.path.dirname(os.path.abspath(__file__))

_ctx = None
if API.startswith("https://"):
    ca = os.path.join(SA_DIR, "ca.crt")
    _ctx = ssl.create_default_context(cafile=ca if os.path.exists(ca) else None)

lock = threading.Lock()
desired = {}          # "ns/deploy" -> {"cpu_m": int, "mem_mb": int}
cache = {"ok": False, "error": "starting", "ts": 0}
activity = []         # recent console actions, newest first
draining = set()


# ---------------------------------------------------------------- k8s helpers
def _token():
    try:
        with open(os.path.join(SA_DIR, "token")) as f:
            return f.read().strip()
    except OSError:
        return ""


def k8s(method, path, body=None, ctype="application/json"):
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Accept": "application/json", "Content-Type": ctype}
    tok = _token()
    if tok:
        headers["Authorization"] = "Bearer " + tok
    req = urllib.request.Request(API + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, context=_ctx, timeout=10) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        try:
            payload = json.loads(e.read())
        except Exception:
            payload = {"message": str(e)}
        return e.code, payload
    except Exception as e:
        return 0, {"message": str(e)}


def items(path):
    st, body = k8s("GET", path)
    if st != 200:
        raise RuntimeError(f"GET {path} -> {st}: {body.get('message', '')}")
    return body.get("items", [])


def log(msg):
    with lock:
        activity.insert(0, {"ts": time.time(), "msg": msg})
        del activity[40:]
    print(msg, flush=True)


# ---------------------------------------------------------------- quantities
_SUF = [("Ki", 2**10), ("Mi", 2**20), ("Gi", 2**30), ("Ti", 2**40),
        ("n", 1e-9), ("u", 1e-6), ("m", 1e-3),
        ("k", 1e3), ("K", 1e3), ("M", 1e6), ("G", 1e9), ("T", 1e12)]


def qty(s):
    if s is None or s == "":
        return 0.0
    s = str(s)
    for suf, mult in _SUF:
        if s.endswith(suf):
            return float(s[: -len(suf)]) * mult
    return float(s)


def pod_requests(pod):
    cpu = mem = 0.0
    for c in pod.get("spec", {}).get("containers", []):
        r = (c.get("resources") or {}).get("requests") or {}
        cpu += qty(r.get("cpu"))
        mem += qty(r.get("memory"))
    return cpu, mem


# ---------------------------------------------------------------- desired state
def load_desired():
    st, cm = k8s("GET", f"/api/v1/namespaces/{SELF_NS}/configmaps/{STATE_CM}")
    if st == 200:
        try:
            d = json.loads((cm.get("data") or {}).get("desired", "{}"))
            with lock:
                desired.update(d)
            log(f"restored desired load for {len(d)} deployment(s)")
        except ValueError:
            log("state ConfigMap unreadable, starting with zero load")


def save_desired():
    with lock:
        snapshot = json.dumps(desired)
    st, body = k8s("PATCH", f"/api/v1/namespaces/{SELF_NS}/configmaps/{STATE_CM}",
                   {"data": {"desired": snapshot}}, "application/merge-patch+json")
    if st == 404:
        st, body = k8s("POST", f"/api/v1/namespaces/{SELF_NS}/configmaps",
                       {"apiVersion": "v1", "kind": "ConfigMap",
                        "metadata": {"name": STATE_CM}, "data": {"desired": snapshot}})
    if st not in (200, 201):
        log(f"could not persist desired state: {st} {body.get('message', '')}")


# ---------------------------------------------------------------- state builder
def pod_status(p):
    md, st = p.get("metadata", {}), p.get("status", {})
    if md.get("deletionTimestamp"):
        return "Terminating"
    if st.get("phase") == "Failed":
        return st.get("reason") or "Failed"
    for cs in st.get("containerStatuses") or []:
        w = (cs.get("state") or {}).get("waiting")
        if w:
            return w.get("reason", "Waiting")
    if st.get("phase") == "Pending":
        for c in st.get("conditions") or []:
            if c.get("type") == "PodScheduled" and c.get("status") == "False":
                return "Unschedulable"
    return st.get("phase", "Unknown")


def qos_from_resources(res):
    req, lim = res.get("requests") or {}, res.get("limits") or {}
    if not req and not lim:
        return "BestEffort"
    if lim and all(lim.get(k) and (req.get(k) in (None, lim.get(k))) for k in ("cpu", "memory")):
        return "Guaranteed"
    return "Burstable"


def build_state():
    nodes_raw = items("/api/v1/nodes")
    active = items("/api/v1/pods?fieldSelector=" + urllib.parse.quote(
        "status.phase!=Succeeded,status.phase!=Failed"))
    demo_raw = [p for p in items("/api/v1/pods?labelSelector=" + urllib.parse.quote(SEL))
                if p["metadata"]["namespace"] in TEAM_NS]

    node_use = {}
    st, nm = k8s("GET", "/apis/metrics.k8s.io/v1beta1/nodes")
    if st == 200:
        for m in nm.get("items", []):
            node_use[m["metadata"]["name"]] = (qty(m["usage"].get("cpu")), qty(m["usage"].get("memory")))
    pod_use = {}
    for ns in TEAM_NS:
        st, pm = k8s("GET", f"/apis/metrics.k8s.io/v1beta1/namespaces/{ns}/pods")
        if st == 200:
            for m in pm.get("items", []):
                cpu = sum(qty(c["usage"].get("cpu")) for c in m.get("containers", []))
                mem = sum(qty(c["usage"].get("memory")) for c in m.get("containers", []))
                pod_use[f"{ns}/{m['metadata']['name']}"] = (cpu, mem)

    req_by_node, count_by_node = {}, {}
    for p in active:
        n = p.get("spec", {}).get("nodeName")
        if not n:
            continue
        c, m = pod_requests(p)
        rc, rm = req_by_node.get(n, (0.0, 0.0))
        req_by_node[n] = (rc + c, rm + m)
        count_by_node[n] = count_by_node.get(n, 0) + 1

    nodes = []
    for n in nodes_raw:
        labels = n["metadata"].get("labels", {})
        if "node-role.kubernetes.io/master" in labels or "node-role.kubernetes.io/control-plane" in labels:
            continue
        name = n["metadata"]["name"]
        conds = {c["type"]: c["status"] for c in n.get("status", {}).get("conditions", [])}
        alloc = n.get("status", {}).get("allocatable", {})
        rc, rm = req_by_node.get(name, (0.0, 0.0))
        uc, um = node_use.get(name, (None, None))
        nodes.append({
            "name": name,
            "zone": labels.get("topology.kubernetes.io/zone", "?"),
            "cordoned": bool(n.get("spec", {}).get("unschedulable")),
            "ready": conds.get("Ready") == "True",
            "memPressure": conds.get("MemoryPressure") == "True",
            "cpuAlloc": qty(alloc.get("cpu")), "memAlloc": qty(alloc.get("memory")),
            "cpuReq": rc, "memReq": rm, "cpuUse": uc, "memUse": um,
            "pods": count_by_node.get(name, 0),
            "draining": name in draining,
        })
    nodes.sort(key=lambda x: (x["zone"], x["name"]))

    pods = []
    for p in demo_raw:
        md, sp, st = p["metadata"], p.get("spec", {}), p.get("status", {})
        cs = (st.get("containerStatuses") or [{}])[0]
        last = ((cs.get("lastState") or {}).get("terminated") or {})
        sched_msg = ""
        for c in st.get("conditions") or []:
            if c.get("type") == "PodScheduled" and c.get("status") == "False":
                sched_msg = c.get("message", "")
        key = f"{md['namespace']}/{md['name']}"
        uc, um = pod_use.get(key, (None, None))
        pods.append({
            "ns": md["namespace"], "name": md["name"],
            "deploy": md.get("labels", {}).get("app", ""),
            "node": sp.get("nodeName") or "",
            "status": pod_status(p),
            "ready": bool(cs.get("ready")),
            "restarts": cs.get("restartCount", 0),
            "lastReason": last.get("reason", ""),
            "qos": st.get("qosClass", ""),
            "cpuUse": uc, "memUse": um,
            "schedMsg": sched_msg,
            "created": md.get("creationTimestamp", ""),
        })
    pods.sort(key=lambda x: (x["ns"], x["name"]))

    deployments = []
    for ns in TEAM_NS:
        for d in items(f"/apis/apps/v1/namespaces/{ns}/deployments?labelSelector=" + urllib.parse.quote(SEL)):
            name = d["metadata"]["name"]
            c0 = d["spec"]["template"]["spec"]["containers"][0]
            res = c0.get("resources") or {}
            mine = [p for p in pods if p["ns"] == ns and p["deploy"] == name]
            with lock:
                want = dict(desired.get(f"{ns}/{name}", {"cpu_m": 0, "mem_mb": 0}))
            deployments.append({
                "ns": ns, "name": name, "key": f"{ns}/{name}",
                "replicas": d["spec"].get("replicas", 0),
                "ready": d.get("status", {}).get("readyReplicas", 0) or 0,
                "requests": res.get("requests") or {}, "limits": res.get("limits") or {},
                "qos": qos_from_resources(res),
                "desired": want,
                "counts": {
                    "running": sum(1 for p in mine if p["status"] == "Running" and p["ready"]),
                    "pending": sum(1 for p in mine if p["status"] in ("Pending", "Unschedulable", "ContainerCreating")),
                    "crashing": sum(1 for p in mine if p["status"] in ("CrashLoopBackOff", "OOMKilled", "Error")),
                    "evicted": sum(1 for p in mine if p["status"] == "Evicted"),
                    "oomRestarts": sum(p["restarts"] for p in mine if p["lastReason"] == "OOMKilled"),
                },
            })

    events = []
    for ns in TEAM_NS:
        st, ev = k8s("GET", f"/api/v1/namespaces/{ns}/events")
        if st == 200:
            for e in ev.get("items", []):
                if e.get("type") != "Warning":
                    continue
                ts = e.get("lastTimestamp") or e.get("eventTime") or e["metadata"].get("creationTimestamp", "")
                events.append({"ts": ts, "ns": ns, "reason": e.get("reason", ""),
                               "obj": (e.get("involvedObject") or {}).get("name", ""),
                               "msg": e.get("message", ""), "count": e.get("count", 1)})
    events.sort(key=lambda x: x["ts"] or "", reverse=True)

    with lock:
        act = list(activity)
    return {"ok": True, "ts": time.time(), "nodes": nodes, "pods": pods,
            "deployments": deployments, "events": events[:25], "activity": act[:20],
            "limits": {"cpu_m": MAX_CPU_M, "mem_mb": MAX_MEM_MB, "replicas": MAX_REPLICAS}}


def refresher():
    while True:
        try:
            s = build_state()
        except Exception as e:
            traceback.print_exc()
            s = {"ok": False, "error": str(e), "ts": time.time()}
        with lock:
            cache.clear()
            cache.update(s)
        time.sleep(REFRESH)


# ---------------------------------------------------------------- actions
def team_ns_ok(ns):
    return ns in TEAM_NS


def set_desired(key, cpu_m, mem_mb):
    ns = key.split("/", 1)[0]
    if not team_ns_ok(ns):
        return 400, {"message": f"{ns} is not a demo namespace"}
    cpu_m = max(0, min(MAX_CPU_M, int(cpu_m)))
    mem_mb = max(0, min(MAX_MEM_MB, int(mem_mb)))
    with lock:
        desired[key] = {"cpu_m": cpu_m, "mem_mb": mem_mb}
    save_desired()
    log(f"load {key}: cpu={cpu_m}m mem={mem_mb}Mi per pod")
    return 200, {"cpu_m": cpu_m, "mem_mb": mem_mb}


def scale(ns, name, replicas):
    if not team_ns_ok(ns):
        return 400, {"message": f"{ns} is not a demo namespace"}
    replicas = max(0, min(MAX_REPLICAS, int(replicas)))
    st, body = k8s("PATCH", f"/apis/apps/v1/namespaces/{ns}/deployments/{name}/scale",
                   {"spec": {"replicas": replicas}}, "application/merge-patch+json")
    log(f"scale {ns}/{name} -> {replicas}" + ("" if st == 200 else f" FAILED {st}: {body.get('message', '')}"))
    return st, body


def cordon(node, on):
    st, body = k8s("PATCH", f"/api/v1/nodes/{node}", {"spec": {"unschedulable": bool(on)}},
                   "application/merge-patch+json")
    log(f"{'cordon' if on else 'uncordon'} {node}" + ("" if st == 200 else f" FAILED {st}: {body.get('message', '')}"))
    return st, body


def drain_worker(node):
    try:
        deadline = time.time() + 300
        while time.time() < deadline:
            pods = [p for p in items(f"/api/v1/pods?labelSelector={urllib.parse.quote(SEL)}"
                                     f"&fieldSelector={urllib.parse.quote('spec.nodeName=' + node)}")
                    if p["metadata"]["namespace"] in TEAM_NS
                    and not p["metadata"].get("deletionTimestamp")
                    and p.get("status", {}).get("phase") not in ("Succeeded", "Failed")]
            if not pods:
                log(f"drain {node}: no demo pods left")
                return
            blocked = 0
            for p in pods:
                ns, name = p["metadata"]["namespace"], p["metadata"]["name"]
                st, body = k8s("POST", f"/api/v1/namespaces/{ns}/pods/{name}/eviction",
                               {"apiVersion": "policy/v1", "kind": "Eviction",
                                "metadata": {"name": name, "namespace": ns}})
                if st in (200, 201):
                    log(f"evicted {ns}/{name}")
                elif st == 429:
                    blocked += 1
                elif st != 404:
                    log(f"evict {ns}/{name} FAILED {st}: {body.get('message', '')}")
            if blocked:
                log(f"drain {node}: {blocked} eviction(s) blocked by PodDisruptionBudget, retrying")
            time.sleep(5)
        log(f"drain {node}: gave up after 5 minutes")
    finally:
        draining.discard(node)


def drain(node):
    if node in draining:
        return 409, {"message": "already draining"}
    st, body = cordon(node, True)
    if st != 200:
        return st, body
    draining.add(node)
    log(f"drain {node}: evicting demo pods (PDBs honoured)")
    threading.Thread(target=drain_worker, args=(node,), daemon=True).start()
    return 202, {"message": "draining"}


# ---------------------------------------------------------------- http
class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else (body if isinstance(body, str) else json.dumps(body)).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _json_body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}")

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(u.query)
        if u.path == "/healthz":
            return self._send(200, "ok", "text/plain")
        if u.path == "/api/desired":
            key = (q.get("key") or [""])[0]
            with lock:
                d = desired.get(key, {"cpu_m": 0, "mem_mb": 0})
            return self._send(200, d)
        if u.path == "/api/state":
            with lock:
                s = dict(cache)
            return self._send(200, s)
        if u.path in ("/", "/index.html"):
            with open(os.path.join(STATIC, "index.html"), "rb") as f:
                return self._send(200, f.read(), "text/html; charset=utf-8")
        self._send(404, {"message": "not found"})

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        try:
            b = self._json_body()
            if path == "/api/load":
                st, body = set_desired(b["key"], b.get("cpu_m", 0), b.get("mem_mb", 0))
            elif path == "/api/scale":
                st, body = scale(b["ns"], b["name"], b["replicas"])
            elif path == "/api/cordon":
                st, body = cordon(b["node"], b.get("on", True))
            elif path == "/api/drain":
                st, body = drain(b["node"])
            else:
                st, body = 404, {"message": "not found"}
        except (KeyError, ValueError) as e:
            st, body = 400, {"message": f"bad request: {e}"}
        self._send(st if st else 502, body)

    def log_message(self, *args):
        pass


def main():
    load_desired()
    threading.Thread(target=refresher, daemon=True).start()
    port = int(os.environ.get("PORT", "8080"))
    log(f"load console listening on :{port}, teams={TEAM_NS}")
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
