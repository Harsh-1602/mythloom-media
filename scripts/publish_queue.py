#!/usr/bin/env python3
"""Cloud publisher for @mythloom (runs in GitHub Actions every 15 min; stdlib only).

queue.json: [{"id", "at": ISO time with offset, "video_url", "caption", "done": false, ...}]
For each item that is due and not done:
  1. duplicate guard: if one of the account's latest media starts with the caption's first line, mark it done (never re-post)
  2. create a REELS container from video_url, poll until FINISHED (max ~12 min), media_publish, read the permalink
  3. record media_id / permalink / published_at in queue.json (the workflow commits it back)
Failures are logged and retried on the next run (up to 8 attempts). Secrets come from env (IG_ACCESS_TOKEN, IG_USER_ID); never printed.
"""
import json, os, sys, time, urllib.parse, urllib.request, urllib.error
from datetime import datetime, timezone

TOKEN, IGID = os.environ["IG_ACCESS_TOKEN"], os.environ["IG_USER_ID"]
G = "https://graph.facebook.com/" + os.environ.get("GRAPH_API_VERSION", "v24.0")
QUEUE = os.environ.get("QUEUE", "queue.json")

def call(method, path, **params):
    params["access_token"] = TOKEN
    q = urllib.parse.urlencode(params)
    r = urllib.request.Request(f"{G}/{path}?{q}" if method == "GET" else f"{G}/{path}",
                               data=None if method == "GET" else q.encode(), method=method)
    try:
        return json.loads(urllib.request.urlopen(r, timeout=120).read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP {e.code} {method} {path}: " + e.read().decode(errors="replace").replace(TOKEN, "***")[:500])

def first_line(c): return next((l.strip() for l in c.splitlines() if l.strip()), "")

def publish(item):
    head = first_line(item["caption"])
    for m in call("GET", f"{IGID}/media", fields="id,caption,permalink,timestamp", limit=10).get("data", []):
        if (m.get("caption") or "").strip().startswith(head):
            return {"media_id": m["id"], "permalink": m.get("permalink"), "published_at": m.get("timestamp"), "note": "already on the account"}
    cid = call("POST", f"{IGID}/media", media_type="REELS", video_url=item["video_url"], caption=item["caption"], share_to_feed="true")["id"]
    print(f"{item['id']}: container {cid}", flush=True)
    for _ in range(72):
        st = call("GET", cid, fields="status_code,status")
        if st.get("status_code") == "FINISHED": break
        if st.get("status_code") in ("ERROR", "EXPIRED"): raise RuntimeError(f"container {st}")
        time.sleep(10)
    else:
        raise RuntimeError("container not FINISHED after 12 min")
    mid = call("POST", f"{IGID}/media_publish", creation_id=cid)["id"]
    m = call("GET", mid, fields="permalink,timestamp")
    return {"media_id": mid, "permalink": m.get("permalink"), "published_at": m.get("timestamp")}

def main():
    if os.environ.get("CHECK") == "workflow_dispatch":   # manual run: prove the secrets work
        print("account check:", call("GET", IGID, fields="username,media_count"))
    q = json.load(open(QUEUE)); now = datetime.now(timezone.utc); changed = False
    for item in q:
        if item.get("done") or datetime.fromisoformat(item["at"]) > now: continue
        if item.get("attempts", 0) >= 8: continue
        item["attempts"] = item.get("attempts", 0) + 1; changed = True
        try:
            res = publish(item); item.update(res, done=True, error=None)
            print(f"{item['id']}: PUBLISHED {res}", flush=True)
        except Exception as e:
            item["error"] = str(e)[:400]; print(f"{item['id']}: attempt {item['attempts']} failed: {item['error']}", flush=True)
        break   # at most one post per run, so two due items never go out back to back
    if changed: json.dump(q, open(QUEUE, "w"), indent=1, ensure_ascii=False); open(QUEUE, "a").write("\n")
    print("queue:", [(i["id"], i["at"], "done" if i.get("done") else "pending") for i in q])

if __name__ == "__main__":
    main()
