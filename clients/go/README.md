# flipr Go client

    import flipr "github.com/janearc/flipr-dist/clients/go"

Its own module, with one dependency, the protobuf runtime the wire is made
of. A service reads its flags through this client, never through code of
its own that posts to flipr.

Every check pings flipr first. The cache saves a trip and is never a
fallback, so when flipr cannot answer, a check says it does not know. What
unknown means is the caller's choice, and a Config without one is refused:

    flipr.Refuse   a gate over spend: unknown means stop
    flipr.Hold     an instrument: carry on, and report the doubt

    fl, err := flipr.New(flipr.Config{
        Service: "metricsd", Version: "v1", EnvPrefix: "METRICSD",
        URL: os.Getenv("METRICSD_FLIPR_URL"), OnUnknown: flipr.Hold,
    })
    on, known, why := fl.Check("smc.enabled")

USING.md covers declarations, env mode, retries, locks and metrics.
../CONTRACT.md is what every flipr client keeps.
