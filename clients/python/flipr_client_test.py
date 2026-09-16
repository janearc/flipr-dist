# tests for the reference client, against a real flipr.
#
# no mocks: FLIPR_TEST_BASE names a live instance (a running deployment, or a
# local binary) and the suite drives it end to end. a client tested against a
# fake of the server it exists for proves the fake, not the client.
#
#     FLIPR_TEST_BASE=http://flipr.test:9800 python3 flipr_client_test.py

import os
import sys
import unittest
import uuid

from flipr_client import (FliprClient, FliprDown, FliprLocked, FlagMissing,
                          ConfigError, Policy, client_header)

BASE = os.environ.get("FLIPR_TEST_BASE", "http://flipr.test:9800")

# the declarations every test client carries (the contract refuses an empty
# list), and the constructor shape in one place
DECLARED = [
    {"key": "fetch.enabled", "value": {"boolValue": True},
     "description": "on: the test fetch runs, one request. off: it does not.",
     "expensive": True},
    {"key": "mode", "value": {"stringValue": "surreal"},
     "description": "which mode the test runs in"},
]


def client(base, service, version, policy=Policy.REFUSE, **kw):
    return FliprClient(base, service, version, declared=DECLARED, policy=policy, **kw)


class ClientAgainstLiveFlipr(unittest.TestCase):
    def setUp(self):
        # a unique version per run keeps tests from seeing each other's
        # namespaces, the same isolation a commit hash gives deploys
        # a unique version per run; not hex, which would read as a commit
        self.c = client(BASE, "client-test", "t-" + uuid.uuid4().hex[:6])

    def test_ping_returns_a_version(self):
        self.assertTrue(self.c.ping())

    def test_publish_then_check(self):
        n = self.c.publish()
        self.assertEqual(n, 2)
        self.assertIs(self.c.check("fetch.enabled"), True)
        self.assertEqual(self.c.check("mode"), "surreal")

    def test_an_undeclared_flag_raises_rather_than_defaulting(self):
        self.c.publish()
        with self.assertRaises(FlagMissing):
            self.c.check("never.declared")

    def test_a_fresh_client_declares_its_namespace_on_first_contact(self):
        # CONTRACT.md section 10: the declarations reach flipr on the first
        # answer, so a client's own namespace is never missing after it has
        # heard from flipr once; refresh on a namespace flipr had never seen
        # declares it rather than raising
        flags = self.c.refresh()
        self.assertEqual(sorted(flags), ["fetch.enabled", "mode"])

    def test_flipr_down_raises_not_serves_stale(self):
        # the ruled semantics: a warm cache does NOT survive flipr going
        # away. point the client at a dead port, warm nothing, and check()
        # must raise rather than answer.
        dead = client("http://127.0.0.1:1", "client-test", "v", timeout=0.5)
        dead._cache = {"fetch.enabled": True}   # a warm cache, planted
        dead._fetched = 0.0                     # stale, forces a refresh
        with self.assertRaises(FliprDown):
            dead.check("fetch.enabled")
        self.assertFalse(dead.last_known)

    def test_hold_returns_the_last_known_and_says_it_is_holding(self):
        # the other policy: an instrument keeps reporting through an outage
        # and PUBLISHES that it is holding
        import time
        held = client("http://127.0.0.1:1", "client-test", "v", policy=Policy.HOLD, timeout=0.5)
        held._cache = {"fetch.enabled": True}
        held._fetched = time.monotonic()
        self.assertIs(held.check("fetch.enabled"), True)
        self.assertFalse(held.last_known)
        self.assertIn("holding last known", held.last_why)
        # with nothing ever read, the declared default
        fresh = client("http://127.0.0.1:1", "client-test", "v", policy=Policy.HOLD, timeout=0.5)
        self.assertIs(fresh.check("fetch.enabled"), True)
        self.assertEqual(fresh.check("mode"), "surreal")

    def test_fresh_cache_still_requires_liveness(self):
        # even inside the ttl the client pings. a fresh cache with flipr gone
        # is exactly the quiet-lie case the ruling forbids.
        import time
        dead = client("http://127.0.0.1:1", "client-test", "v", timeout=0.5)
        dead._cache = {"fetch.enabled": True}
        dead._fetched = time.monotonic()        # fresh, skips the refresh
        with self.assertRaises(FliprDown):
            dead.check("fetch.enabled")



class SelfHealAcrossAWipe(unittest.TestCase):
    # the ruling under test: if flipr is wiped, the services that use it
    # write their defaults back. flipr knows when it has been wiped, and it
    # heals itself.
    #
    # this suite needs to WIPE the store, which it cannot do to the shared
    # deployment -- so it drives its own flipr binary on a scratch db and
    # kills it mid-test. skipped when the binary is absent.

    def _start(self, db):
        import subprocess, shutil, tempfile, time as t
        binary = os.environ.get("FLIPR_BINARY", "")
        if not binary or not os.path.exists(binary):
            self.skipTest("FLIPR_BINARY not set; wipe test needs its own instance")
        proc = subprocess.Popen(
            [binary, "-db", db, "-addr", "127.0.0.1:15177"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # a probe under its own name, so it declares nothing into healer@v1
        c = client("http://127.0.0.1:15177", "probe", "v1", cache_ttl=0.0)
        for _ in range(50):
            try:
                c.ping(); break
            except FliprDown:
                t.sleep(0.1)
        return proc

    def test_a_wiped_flipr_is_healed_by_its_client(self):
        import tempfile, os as o, time as t
        d = tempfile.mkdtemp()
        db = o.path.join(d, "flipr.db")

        proc = self._start(db)
        try:
            c = FliprClient("http://127.0.0.1:15177", "healer", "v1", cache_ttl=0.0,
                            policy=Policy.REFUSE,
                            declared=[{"key": "fetch.enabled",
                                       "value": {"boolValue": False},
                                       "description": "on: the heal test fetches. off: it does not.",
                                       "expensive": True}])
            c.publish()
            self.assertIs(c.check("fetch.enabled"), False)

            # THE WIPE: kill flipr, delete its store, restart it empty.
            proc.kill(); proc.wait()
            o.remove(db)
            proc = self._start(db)

            # without the heal this read raises FlagMissing against a
            # healthy-looking flipr -- the exact failure named above. with it,
            # the client sees the new generation, re-publishes its declared
            # defaults, and the read SUCCEEDS.
            self.assertIs(c.check("fetch.enabled"), False)
        finally:
            proc.kill(); proc.wait()

    def test_a_client_that_never_published_still_fails_loudly(self):
        import tempfile, os as o
        d = tempfile.mkdtemp()
        proc = self._start(o.path.join(d, "flipr.db"))
        try:
            # a read-only consumer has nothing to heal with; FlagMissing is
            # correct and loud, not a regression.
            c = client("http://127.0.0.1:15177", "reader", "v1", cache_ttl=0.0)
            with self.assertRaises(FlagMissing):
                c.check("never.declared")
        finally:
            proc.kill(); proc.wait()



class AbsentButRouted(unittest.TestCase):
    # traefik answers 404 for a route with nothing behind it, and that 404 is
    # a SUCCESS to curl and an HTTPError-with-body to urllib. it must read as
    # FliprDown, never FlagMissing: "the flag is not declared" and "there is
    # no flipr" demand opposite responses from a caller.

    def test_a_plaintext_404_is_down_not_missing(self):
        import http.server, threading

        class Traefik404(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers.get('Content-Length') or 0))
                body = b"404 page not found\n"
                self.send_response(404)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(body)
            def log_message(self, *a):
                pass

        srv = http.server.HTTPServer(("127.0.0.1", 0), Traefik404)
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        try:
            c = client(f"http://127.0.0.1:{srv.server_port}", "svc", "v")
            with self.assertRaises(FliprDown):
                c.ping()
            with self.assertRaises(FliprDown):
                c.check("anything")
        finally:
            srv.shutdown()


class TheContract(unittest.TestCase):
    # clients/CONTRACT.md, the parts a fake edge can prove without flipr

    def _serve(self, handler):
        import http.server, threading
        srv = http.server.HTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.addCleanup(srv.shutdown)
        return srv

    def test_every_request_names_the_caller_and_the_client(self):
        import http.server
        seen = {}

        class Recorder(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers.get('Content-Length') or 0))
                seen["caller"] = self.headers.get("X-Flipr-Caller")
                seen["client"] = self.headers.get("X-Flipr-Client")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"storeGeneration":"g1","version":"t"}')
            def log_message(self, *a):
                pass

        srv = self._serve(Recorder)
        client(f"http://127.0.0.1:{srv.server_port}", "svc", "v1").ping()
        self.assertEqual(seen["caller"], "svc@v1")
        self.assertEqual(seen["client"], client_header())
        self.assertTrue(seen["client"].startswith("py/"))

    def test_a_lock_is_its_own_outcome_and_is_remembered(self):
        import http.server, time
        calls = []

        class Locked(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers.get('Content-Length') or 0))
                calls.append(time.monotonic())
                self.send_response(423)
                self.send_header("Content-Type", "application/json")
                self.send_header("Retry-After", "1")
                self.end_headers()
                self.wfile.write(b'{"error":"locked: restore to 4127"}')
            def log_message(self, *a):
                pass

        srv = self._serve(Locked)
        c = client(f"http://127.0.0.1:{srv.server_port}", "svc", "v1", policy=Policy.HOLD)
        # HOLD: the declared default, the lock reported, ONE request (a lock
        # is not retried inside the call)
        self.assertIs(c.check("fetch.enabled"), True)
        self.assertFalse(c.last_known)
        self.assertEqual(c.locked(), (True, "locked: restore to 4127"))
        self.assertEqual(len(calls), 1)
        # inside the deadline: the policy's answer, no request
        self.assertIs(c.check("fetch.enabled"), True)
        self.assertEqual(len(calls), 1)
        # after it: exactly one request, and a new deadline
        time.sleep(1.1)
        c.check("fetch.enabled")
        self.assertEqual(len(calls), 2)
        c.check("fetch.enabled")
        self.assertEqual(len(calls), 2)
        # REFUSE: FliprLocked, which is a FliprDown, from the remembered lock too
        r = client(f"http://127.0.0.1:{srv.server_port}", "svc", "v1")
        with self.assertRaises(FliprLocked):
            r.check("fetch.enabled")
        with self.assertRaises(FliprDown):
            r.check("fetch.enabled")

    def test_an_edge_404_is_retried_and_flipr_s_own_is_not(self):
        import http.server
        counts = {"edge": 0, "own": 0}

        class Edge(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers.get('Content-Length') or 0))
                counts["edge"] += 1
                self.send_response(404); self.send_header("Content-Type", "text/plain"); self.end_headers()
                self.wfile.write(b"404 page not found")
            def log_message(self, *a): pass

        class Own(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers.get('Content-Length') or 0))
                counts["own"] += 1
                self.send_response(404); self.send_header("Content-Type", "application/json"); self.end_headers()
                self.wfile.write(b'{"error":"no namespace svc@v1"}')
            def log_message(self, *a): pass

        e = self._serve(Edge)
        with self.assertRaises(FliprDown):
            client(f"http://127.0.0.1:{e.server_port}", "svc", "v1").ping()
        self.assertEqual(counts["edge"], 3)
        o = self._serve(Own)
        with self.assertRaises(FlagMissing):
            client(f"http://127.0.0.1:{o.server_port}", "svc", "v1").refresh()
        self.assertEqual(counts["own"], 1)  # flipr's own 404 is the contract speaking, once

    def test_a_boot_that_finds_flipr_down_publishes_on_the_first_answer(self):
        # CONTRACT.md section 10: the declarations reach flipr on the first
        # answer. a fake that refuses the first check and answers the second,
        # counting publishes: exactly one, on the second.
        import http.server, json as _json
        state = {"down": True, "publishes": 0}

        class FlakyBoot(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers.get('Content-Length') or 0))
                method = self.path.rsplit("/", 1)[-1]
                if state["down"]:
                    self.send_response(503); self.end_headers(); self.wfile.write(b"down"); return
                body = {"storeGeneration": "g1", "version": "t"}
                if method == "PublishNamespace":
                    state["publishes"] += 1
                    body = {"flagsPublished": 2}
                elif method == "GetNamespace":
                    body = {"namespace": {"service": "svc", "version": "v1", "flags": [
                        {"key": "fetch.enabled", "value": {"boolValue": True}}]}}
                self.send_response(200); self.send_header("Content-Type", "application/json"); self.end_headers()
                self.wfile.write(_json.dumps(body).encode())
            def log_message(self, *a): pass

        srv = self._serve(FlakyBoot)
        c = client(f"http://127.0.0.1:{srv.server_port}", "svc", "v1")
        with self.assertRaises(FliprDown):
            c.publish()                      # the startup publish, refused by the outage
        with self.assertRaises(FliprDown):
            c.check("fetch.enabled")
        self.assertEqual(state["publishes"], 0)
        state["down"] = False
        self.assertIs(c.check("fetch.enabled"), True)
        self.assertEqual(state["publishes"], 1)   # published on the first answer
        c.check("fetch.enabled")
        self.assertEqual(state["publishes"], 1)   # and not again

    def test_construction_refusals(self):
        with self.assertRaises(ConfigError):
            FliprClient("http://flipr.test", "svc", "29c44ff", declared=DECLARED, policy=Policy.REFUSE)
        with self.assertRaises(ConfigError):
            FliprClient("http://flipr.test", "svc", "v1", declared=[], policy=Policy.REFUSE)
        with self.assertRaises(ConfigError):
            FliprClient("http://flipr.test", "svc", "v1", declared=DECLARED, policy=None)
        with self.assertRaises(ConfigError):
            FliprClient("http://10.0.0.5:9800", "svc", "v1", declared=DECLARED, policy=Policy.REFUSE)
        # a name with a port is fine: the throwaway edge is flipr.test:7800
        FliprClient("http://flipr.test:7800", "svc", "v1", declared=DECLARED, policy=Policy.REFUSE)

if __name__ == "__main__":
    print(f"against {BASE}", file=sys.stderr)
    unittest.main()
