# flipr Python client

Standard library only, and it stays that way: a service that reads one flag
should not inherit flipr's server dependencies. Install it from the
repository at a pinned tag, subdirectory `clients/python`.

    import flipr_client
    from flipr_client import Policy

    flags = flipr_client.FliprClient(
        "http://flipr.test", "kingfisher", "v1",
        declared=DECLARED, policy=Policy.REFUSE)
    flags.publish()
    if flags.check("fetch.enabled"):
        do_the_expensive_fetch()

Every check pings flipr first; the cache saves a trip and is never a
fallback. When flipr cannot answer, the policy decides: `REFUSE` for a gate
over spend, HOLD for an instrument that must keep reporting.

`DECLARED` is the service's flag list. The version is the flag contract's,
pinned, never a commit; the base is a name, never an address.

USING.md has the rest: installing, declarations, retries, and why this is
a package. ../CONTRACT.md is what every flipr client keeps.
