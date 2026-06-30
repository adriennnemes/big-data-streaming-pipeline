import os
import sys
import time
import requests

TFC_HOST_DEFAULT = "https://app.terraform.io"


def tfc_get(session: requests.Session, url: str) -> dict:
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def must_env(name: str, default:str=None) -> str:
    v = os.environ.get(name, default)
    if not v:
        print(f"Missing required env var {name}", file=sys.stderr)
        sys.exit(1)
    return v

def normalize_bootstrap_servers(value: str) -> str:
    if not value:
        return value

    parts = [p.strip() for p in value.split(",") if p.strip()]
    cleaned = []
    for p in parts:
        # strip protocol prefix if present
        if "://" in p:
            p = p.split("://", 1)[1]
        cleaned.append(p)
    return ",".join(cleaned)

def main():
    token = must_env("TFC_TOKEN")
    org = must_env("TFC_ORG")
    workspace = must_env("TFC_WORKSPACE")

    host = os.environ.get("TFC_HOST", TFC_HOST_DEFAULT).rstrip("/")
    out_path = os.environ.get("ENV_OUT", ".env")

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",                 # required :contentReference[oaicite:4]{index=4}
        "Content-Type": "application/vnd.api+json",
    })

    # Resolve workspace -> workspace_id
    ws_url = f"{host}/api/v2/organizations/{org}/workspaces/{workspace}"
    ws = tfc_get(session, ws_url)
    workspace_id = ws["data"]["id"]

    # List current outputs (retry 503 "processing")
    outs_url = f"{host}/api/v2/workspaces/{workspace_id}/current-state-version-outputs"
    for attempt in range(1, 6):
        try:
            outs = tfc_get(session, outs_url)["data"]
            break
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 503 and attempt < 5:
                time.sleep(2 * attempt)
                continue
            raise

    outputs_by_name = {}
    for item in outs:
        name = item["attributes"]["name"]
        sensitive = item["attributes"]["sensitive"]
        value = item["attributes"].get("value")

        # Sensitive outputs are null here -> fetch via state-version-outputs :contentReference[oaicite:5]{index=5}
        if sensitive and value is None:
            out_id = item["id"]
            show_url = f"{host}/api/v2/state-version-outputs/{out_id}"
            show = tfc_get(session, show_url)
            value = show["data"]["attributes"].get("value")

        outputs_by_name[name] = value

    env_map = {
        "KAFKA_BOOTSTRAP_SERVERS": normalize_bootstrap_servers(str(outputs_by_name.get("kafka_bootstrap_servers") or "")),
        "KAFKA_API_KEY": outputs_by_name.get("kafka_admin_api_key"),
        "KAFKA_API_SECRET": outputs_by_name.get("kafka_admin_api_secret"),
        "TOPIC_RAW": outputs_by_name.get("kafka_topic_raw"),
        "TOPIC_AGGREGATES": outputs_by_name.get("kafka_topic_aggregates"),
        "TOPIC_ALERTS": outputs_by_name.get("kafka_topic_alerts"),
    }

    missing = [k for k, v in env_map.items() if not v]
    if missing:
        print("Missing values for:", ", ".join(missing), file=sys.stderr)
        print("Did you apply successfully and define matching outputs.tf names?", file=sys.stderr)
        sys.exit(2)

    with open(out_path, "w", encoding="utf-8") as f:
        for k, v in env_map.items():
            v_str = str(v).replace('"', '\\"')
            f.write(f'{k}="{v_str}"\n')

    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
