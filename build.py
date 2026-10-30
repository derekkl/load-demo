"""Generate load-demo.yaml from worker.py, console.py and index.html."""
import os
import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
read = lambda f: open(os.path.join(HERE, f)).read()

IMAGE = "registry.access.redhat.com/ubi9/python-311"
PART_OF = "load-demo"
CONSOLE_NS = "load-console"
TEAMS = {
    # the "good" team: requests == limits (Guaranteed), quota, limitrange, PDB, spread
    "team-guarded": {"deploy": "guarded", "resources": {
        "requests": {"cpu": "500m", "memory": "1Gi"},
        "limits": {"cpu": "500m", "memory": "1Gi"}}},
    # the "bad" team: nothing set (BestEffort), no PDB, no spread
    "team-wild": {"deploy": "wild", "resources": {}},
}
SEC = {"allowPrivilegeEscalation": False, "runAsNonRoot": True,
       "capabilities": {"drop": ["ALL"]}, "seccompProfile": {"type": "RuntimeDefault"}}


class Lit(str):
    pass


yaml.add_representer(Lit, lambda d, s: d.represent_scalar("tag:yaml.org,2002:str", s, style="|"))
yaml.add_representer(str, lambda d, s: d.represent_scalar("tag:yaml.org,2002:str", s,
                                                          style="|" if "\n" in s else None))


def labels(app):
    return {"app": app, "app.kubernetes.io/part-of": PART_OF}


def env_downward():
    f = lambda p: {"valueFrom": {"fieldRef": {"fieldPath": p}}}
    return [{"name": "POD_NAME", **f("metadata.name")},
            {"name": "POD_NAMESPACE", **f("metadata.namespace")},
            {"name": "NODE_NAME", **f("spec.nodeName")}]


docs = []
add = docs.append

# ------------------------------------------------------------- namespaces
add({"apiVersion": "v1", "kind": "Namespace",
     "metadata": {"name": CONSOLE_NS, "labels": {"app.kubernetes.io/part-of": PART_OF + "-console"}}})
for ns in TEAMS:
    add({"apiVersion": "v1", "kind": "Namespace",
         "metadata": {"name": ns, "labels": {"app.kubernetes.io/part-of": PART_OF}}})

# ------------------------------------------------------------- console RBAC
add({"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": "load-console", "namespace": CONSOLE_NS}})
add({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
     "metadata": {"name": "load-console"},
     "rules": [
         {"apiGroups": [""], "resources": ["nodes"], "verbs": ["get", "list", "patch"]},
         {"apiGroups": [""], "resources": ["pods", "events"], "verbs": ["get", "list"]},
         {"apiGroups": ["apps"], "resources": ["deployments"], "verbs": ["get", "list"]},
         {"apiGroups": ["metrics.k8s.io"], "resources": ["nodes", "pods"], "verbs": ["get", "list"]},
     ]})
add({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
     "metadata": {"name": "load-console"},
     "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "load-console"},
     "subjects": [{"kind": "ServiceAccount", "name": "load-console", "namespace": CONSOLE_NS}]})
add({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
     "metadata": {"name": "load-console-state", "namespace": CONSOLE_NS},
     "rules": [{"apiGroups": [""], "resources": ["configmaps"], "verbs": ["get", "create", "patch"]}]})
add({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
     "metadata": {"name": "load-console-state", "namespace": CONSOLE_NS},
     "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "load-console-state"},
     "subjects": [{"kind": "ServiceAccount", "name": "load-console", "namespace": CONSOLE_NS}]})
for ns in TEAMS:  # write actions only inside the team namespaces
    add({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
         "metadata": {"name": "load-console", "namespace": ns},
         "rules": [{"apiGroups": [""], "resources": ["pods/eviction"], "verbs": ["create"]},
                   {"apiGroups": ["apps"], "resources": ["deployments/scale"], "verbs": ["get", "patch"]}]})
    add({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
         "metadata": {"name": "load-console", "namespace": ns},
         "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "load-console"},
         "subjects": [{"kind": "ServiceAccount", "name": "load-console", "namespace": CONSOLE_NS}]})

# ------------------------------------------------------------- console app
add({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "load-console-app", "namespace": CONSOLE_NS},
     "data": {"console.py": Lit(read("console.py")), "index.html": Lit(read("index.html"))}})
add({"apiVersion": "apps/v1", "kind": "Deployment",
     "metadata": {"name": "load-console", "namespace": CONSOLE_NS, "labels": {"app": "load-console"}},
     "spec": {"replicas": 1, "selector": {"matchLabels": {"app": "load-console"}},
              "template": {"metadata": {"labels": {"app": "load-console"}}, "spec": {
                  "serviceAccountName": "load-console",
                  "terminationGracePeriodSeconds": 5,
                  "containers": [{
                      "name": "console", "image": IMAGE,
                      "command": ["python3", "/app/console.py"],
                      "env": [{"name": "TEAM_NAMESPACES", "value": ",".join(TEAMS)},
                              {"name": "MAX_CPU_M", "value": "4000"},
                              {"name": "MAX_MEM_MB", "value": "6144"},
                              {"name": "MAX_REPLICAS", "value": "60"},
                              {"name": "POD_NAMESPACE", "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}}}],
                      "ports": [{"containerPort": 8080, "name": "http"}],
                      "readinessProbe": {"httpGet": {"path": "/healthz", "port": 8080}, "periodSeconds": 5},
                      # Guaranteed QoS so the console is the last thing the kubelet evicts under pressure
                      "resources": {"requests": {"cpu": "200m", "memory": "256Mi"},
                                    "limits": {"cpu": "200m", "memory": "256Mi"}},
                      "volumeMounts": [{"name": "app", "mountPath": "/app"}],
                      "securityContext": SEC}],
                  "volumes": [{"name": "app", "configMap": {"name": "load-console-app"}}]}}}})
add({"apiVersion": "v1", "kind": "Service", "metadata": {"name": "load-console", "namespace": CONSOLE_NS},
     "spec": {"selector": {"app": "load-console"}, "ports": [{"name": "http", "port": 8080, "targetPort": 8080}]}})
add({"apiVersion": "route.openshift.io/v1", "kind": "Route", "metadata": {"name": "load-console", "namespace": CONSOLE_NS},
     "spec": {"to": {"kind": "Service", "name": "load-console"}, "port": {"targetPort": "http"},
              "tls": {"termination": "edge", "insecureEdgeTerminationPolicy": "Redirect"}}})

# ------------------------------------------------------------- teams
for ns, t in TEAMS.items():
    app = t["deploy"]
    add({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "worker-app", "namespace": ns},
         "data": {"worker.py": Lit(read("worker.py"))}})
    pod_spec = {
        "terminationGracePeriodSeconds": 10,
        "containers": [{
            "name": "worker", "image": IMAGE,
            "command": ["python3", "/app/worker.py"],
            "env": env_downward() + [{"name": "DEPLOY_NAME", "value": app}],
            "ports": [{"containerPort": 8080, "name": "http"}],
            "readinessProbe": {"httpGet": {"path": "/healthz", "port": 8080}, "periodSeconds": 5},
            "resources": t["resources"],
            "volumeMounts": [{"name": "app", "mountPath": "/app"}],
            "securityContext": SEC}],
        "volumes": [{"name": "app", "configMap": {"name": "worker-app"}}],
    }
    if ns == "team-guarded":
        pod_spec["topologySpreadConstraints"] = [{
            "maxSkew": 1, "topologyKey": "topology.kubernetes.io/zone", "whenUnsatisfiable": "ScheduleAnyway",
            "labelSelector": {"matchLabels": {"app": app}}}]
    add({"apiVersion": "apps/v1", "kind": "Deployment",
         "metadata": {"name": app, "namespace": ns, "labels": labels(app)},
         "spec": {"replicas": 12, "selector": {"matchLabels": {"app": app}},
                  "template": {"metadata": {"labels": labels(app)}, "spec": pod_spec}}})

# guardrails for the guarded team only
g = "team-guarded"
add({"apiVersion": "v1", "kind": "ResourceQuota", "metadata": {"name": "team-quota", "namespace": g},
     "spec": {"hard": {"requests.cpu": "30", "limits.cpu": "30",
                       "requests.memory": "60Gi", "limits.memory": "60Gi", "pods": "60"}}})
add({"apiVersion": "v1", "kind": "LimitRange", "metadata": {"name": "team-defaults", "namespace": g},
     "spec": {"limits": [{"type": "Container",
                          "default": {"cpu": "500m", "memory": "1Gi"},
                          "defaultRequest": {"cpu": "500m", "memory": "1Gi"},
                          "max": {"cpu": "2", "memory": "4Gi"}}]}})
add({"apiVersion": "policy/v1", "kind": "PodDisruptionBudget", "metadata": {"name": "guarded", "namespace": g},
     "spec": {"maxUnavailable": "25%", "selector": {"matchLabels": {"app": "guarded"}}}})

header = """# ARO POC - load demo: proper resource limits vs none, under load and rescheduling
#
# Paste into  + > Import YAML  (or: oc apply -f load-demo.yaml)
# Console:    oc get route load-console -n load-console   (open via your SOCKS tunnel)
# Cleanup:    oc delete project load-console team-guarded team-wild
#             oc delete clusterrole,clusterrolebinding load-console
#
# team-guarded: requests == limits (Guaranteed QoS), ResourceQuota, LimitRange, PDB, zone spread
# team-wild:    no requests, no limits (BestEffort), no PDB, no spread
# Both run the identical worker; the console sets CPU/memory load per pod.
"""
out = os.path.join(HERE, "load-demo.yaml")
with open(out, "w") as f:
    f.write(header)
    for d in docs:
        f.write("---\n")
        f.write(yaml.dump(d, sort_keys=False, width=1000, allow_unicode=True))
print(f"wrote {out}: {len(docs)} resources")
