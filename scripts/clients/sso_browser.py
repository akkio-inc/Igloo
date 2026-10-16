"""A scripted browser for scripts/e2e-sso.sh: signs in to Igloo's web
console through Keycloak, as a person would, and does what the test needs
next. Each command prints one JSON object. Cookies are kept in a file per
user between commands. Standard library only.

    sso_browser.py signin USER PASSWORD   sign in with single sign-on
    sso_browser.py session USER           the console session's state
    sso_browser.py token USER NAME        CREATE TOKEN in the console
    sso_browser.py access-token USER PASSWORD
                                          an access token from Keycloak
    sso_browser.py remove-group USER GROUP
                                          remove USER from a Keycloak group

E2E_CA is the CA file both servers' certificates chain to, E2E_JARS the
directory of cookie files, E2E_KEYCLOAK_ADMIN_PASSWORD Keycloak's admin
password.
"""

import html
import http.cookiejar
import json
import os
import re
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request

CONSOLE = "https://igloo:8443"
KEYCLOAK = "https://keycloak:8443"
REALM = KEYCLOAK + "/realms/igloo"
CLIENT_ID, CLIENT_SECRET = "igloo", "e2e-only-client-secret"

context = ssl.create_default_context(cafile=os.environ["E2E_CA"])


def jar_for(user):
    jar = http.cookiejar.MozillaCookieJar(os.path.join(os.environ["E2E_JARS"], user + ".cookies"))
    if os.path.exists(jar.filename):
        jar.load(ignore_discard=True, ignore_expires=True)
    return jar


def opener(jar=None):
    handlers = [urllib.request.HTTPSHandler(context=context)]
    if jar is not None:
        handlers.append(urllib.request.HTTPCookieProcessor(jar))
    return urllib.request.build_opener(*handlers)


def request(o, url, data=None, headers=None, method=None):
    """Returns (status, final URL, body), whatever the status."""
    if isinstance(data, dict):
        data = urllib.parse.urlencode(data).encode()
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    try:
        with o.open(req, timeout=60) as resp:
            return resp.status, resp.geturl(), resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.geturl(), e.read().decode()


def signin(user, password):
    jar = jar_for(user)
    jar.clear()
    o = opener(jar)
    status, url, page = request(o, CONSOLE + "/sso/start")
    if url.startswith(KEYCLOAK):
        form = re.search(r'<form[^>]*id="kc-form-login"[^>]*>', page)
        action = form and re.search(r'action="([^"]+)"', form.group(0))
        if not action:
            return {"result": "error", "status": status, "url": url, "page": page[:500]}
        status, url, page = request(o, html.unescape(action.group(1)),
                                    {"username": user, "password": password, "credentialId": ""})
    jar.save(ignore_discard=True, ignore_expires=True)
    if url.startswith(KEYCLOAK):
        # Keycloak kept the browser: it refused the sign-in.
        message = re.search(r'class="[^"]*(?:kc-feedback-text|alert-error|pf-v5-c-alert__title)[^"]*"[^>]*>\s*([^<]+)', page)
        return {"result": "idp", "message": html.unescape(message.group(1)).strip() if message else page[:300]}
    query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
    if "sso_error" in query:
        return {"result": "refused", "code": query["sso_error"][0]}
    return dict(result="signed_in", **session(user))


def session(user):
    status, _, body = request(opener(jar_for(user)), CONSOLE + "/api/session", headers={"Accept": "application/json"})
    out = {"status": status}
    if status == 200:
        info = json.loads(body)
        out.update(user=info["user"], role=info["role"], method=info["method"])
    return out


def token(user, name):
    jar = jar_for(user)
    o = opener(jar)
    status, _, body = request(o, CONSOLE + "/api/session", headers={"Accept": "application/json"})
    if status != 200:
        return {"result": "signed_out", "status": status}
    csrf = json.loads(body)["csrf"]
    status, _, body = request(o, CONSOLE + "/api/query", data=json.dumps({"sql": "CREATE TOKEN " + name}).encode(),
                              headers={"Content-Type": "application/json", "X-CSRF-Token": csrf, "Origin": CONSOLE})
    found = re.search(r"igloo_[a-z2-7]{68}", body)
    return {"result": "created", "token": found.group(0)} if found else {"result": "error", "status": status, "body": body[:500]}


def access_token(user, password):
    status, _, body = request(opener(), REALM + "/protocol/openid-connect/token", {
        "grant_type": "password", "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET,
        "username": user, "password": password, "scope": "openid email profile"})
    if status != 200:
        return {"result": "error", "status": status, "body": body[:300]}
    return {"result": "issued", "access_token": json.loads(body)["access_token"]}


def remove_group(user, group):
    o = opener()
    _, _, body = request(o, KEYCLOAK + "/realms/master/protocol/openid-connect/token", {
        "grant_type": "password", "client_id": "admin-cli", "username": "admin",
        "password": os.environ["E2E_KEYCLOAK_ADMIN_PASSWORD"]})
    auth = {"Authorization": "Bearer " + json.loads(body)["access_token"]}
    admin = KEYCLOAK + "/admin/realms/igloo"
    _, _, body = request(o, admin + "/users?exact=true&username=" + urllib.parse.quote(user), headers=auth)
    user_id = json.loads(body)[0]["id"]
    _, _, body = request(o, admin + "/groups?search=" + urllib.parse.quote(group), headers=auth)
    group_id = next(g["id"] for g in json.loads(body) if g["name"] == group)
    status, _, _ = request(o, admin + "/users/" + user_id + "/groups/" + group_id, headers=auth, method="DELETE")
    return {"result": "removed" if status == 204 else "error", "status": status}


COMMANDS = {"signin": signin, "session": session, "token": token, "access-token": access_token, "remove-group": remove_group}

if __name__ == "__main__":
    print(json.dumps(COMMANDS[sys.argv[1]](*sys.argv[2:])))
