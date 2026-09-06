# ModelGate

[![CI](https://github.com/HuzaifaAbdulRehman/model-gate/actions/workflows/ci.yml/badge.svg)](https://github.com/HuzaifaAbdulRehman/model-gate/actions/workflows/ci.yml)

ModelGate is an OpenAI-compatible gateway that keeps an LLM feature available
when a provider fails, limits tenants by token cost, and records a redacted
audit trail.

```text
application
    |
    v
ModelGate --> provider A
    |            |
    |            +-- failure before output --> provider B
    |
    +-- Redis: token budget and short-lived completion cache
    +-- PostgreSQL: redacted request, response and attempt history
```

The difficult boundary is the first content byte. Before it, ModelGate can
discard buffered metadata and try another provider without changing what the
caller sees. After it, HTTP 200 is final and replaying the request would
duplicate text already displayed. ModelGate ends that stream with an explicit
SSE error and `finish_reason: "modelgate_interrupted"`.

## Run it locally

You need Node.js 24 and Docker. The commands below were run on a clean local
checkout.

```bash
git clone https://github.com/HuzaifaAbdulRehman/model-gate.git
cd model-gate
npm ci
cp .env.example .env
docker compose up -d --wait
npm run migrate:up
npm test
```

On PowerShell, use `Copy-Item .env.example .env` instead of `cp`.

For a visible failover and interruption demo, run:

```bash
npm run demo
```

The script starts isolated mock providers and two gateways. It prints a clean
pre-commit failover to `mock-backup`, then a post-commit failure from
`mock-primary` with its explicit terminal error. It exits non-zero if either
guarantee breaks. Run `docker compose down` when finished.

For the one-minute talk track and the longer design explanation, see
[`docs/INTERVIEW.md`](docs/INTERVIEW.md).

## What is implemented

- Retry and ordered provider failover for regular and streaming completions.
- A hard request deadline that aborts in-flight provider work. If a stream has
  already committed, the timeout is reported inside the stream.
- A streaming commit point with backpressure and client-disconnect handling.
- Explicit terminal frames for failures after the response is committed.
- A Redis token bucket that reserves the worst case before admission and
  reconciles after completion.
- Periodic whole-prefix token counting during streams, followed by provider
  usage when the provider sends it.
- Exact-match, tenant-scoped completion caching.
- PostgreSQL audit rows for the request and every provider attempt, including
  provider request IDs and the bytes and tokens sent during a stream.
- Format-based redaction for credentials, contact details, payment cards,
  national identifiers and IP addresses.

The test suite uses TCP mock providers that can hang, rate-limit, truncate a
frame, close cleanly without a terminal frame, or reset a socket at a selected
content offset. It currently contains 231 unit and integration tests. One test
holds a downstream write open and verifies the gateway stops pulling upstream
until the client drains.

## Measurements

Run `npm run bench` to reproduce the benchmark. It creates an isolated
`modelgate_bench` database, uses Redis database 2, starts the gateway and mock
providers on ephemeral ports, warms connections, and cleans its rows and Redis
keys afterward.

This run used Node.js 24.13.0 on Windows 10 with an Intel i5-8350U. The provider
was a loopback deterministic mock, so these numbers measure gateway overhead
and failure handling. They do not predict internet or model latency.

| Measurement | p50 | p99 | Samples |
|---|---:|---:|---:|
| Direct provider request | 1.45 ms | 4.08 ms | 200 |
| Cold gateway request | 7.70 ms | 13.51 ms | 200 |
| Paired gateway overhead | 6.24 ms | 11.62 ms | 200 |
| Gateway cache hit | 2.97 ms | 5.46 ms | 200 |
| Direct streaming TTFT | 2.34 ms | 4.46 ms | 100 |
| Healthy gateway TTFT | 5.42 ms | 7.99 ms | 100 |
| Pre-commit failover TTFT | 8.14 ms | 12.72 ms | 100 |

All 100 pre-commit failover samples matched the backup output exactly. All 50
post-commit failures ended with an error event, an interruption finish reason,
and `[DONE]`; none stopped silently.

The benchmark also reports token-estimate disagreement against the mock. That
is a harness diagnostic, not provider ground truth.

### Live Groq check

`npm run validate:groq` requires `GROQ_API_KEY` in `.env`. It uses
`openai/gpt-oss-20b`, which Groq currently lists as a production model, and
sends only synthetic prompts. The script uses an isolated database and Redis
namespace, then removes its data. See Groq's
[model page](https://console.groq.com/docs/model/openai/gpt-oss-20b) and
[prefill documentation](https://console.groq.com/docs/prefilling).

On 6 September 2026, all 26 requests passed through ModelGate and all 26 audit
rows used provider-reported usage. The original prompt estimate was 63 to 66
tokens below Groq across five prompt shapes. A model-specific 64-token offset
reduced the residual to -2 through +1 tokens: 0% median absolute error and
2.04% maximum.

The prefill experiment ended the supplied prefix halfway through a word. In all
20 trials, Groq returned the full prefix followed by the expected remainder.
A gateway would have to remove that overlap before forwarding the response.
This exact-output test does not establish coherent continuation for an
open-ended answer, so ModelGate still terminates an interrupted stream
explicitly.

## Boundaries

Cross-provider continuation after content has reached the caller is not
attempted. A second model cannot recover the first model's sampling state, and
we do not have live evidence that a synthetic continuation prompt avoids seams.
The caller receives an explicit interruption instead.

PostgreSQL stores redacted prompts and completions. Redis stores raw completion
text for cache hits with a short TTL and persistence disabled. The redactor
recognises structured formats; it cannot reliably detect names, street
addresses, or free-text descriptions of a person.

ModelGate currently uses one static gateway key and one tenant. It does not
include billing, semantic caching, a dashboard, or a public deployment. Local
performance numbers use the deterministic mock; Groq is used only by the
opt-in validation command.
