# ModelGate interview guide

## 60-second demo

Start the dependencies and run the deterministic demo:

```bash
docker compose up -d --wait
npm run demo
```

Use this talk track while the command runs:

> ModelGate is an OpenAI-compatible gateway for applications that cannot depend on one
> LLM provider. This first request shows a clean failover: the primary provider fails
> before any content reaches the caller, so the gateway retries the backup and returns
> one complete response. The second request fails after six content deltas. At that
> point HTTP 200 and some text are already visible, so retrying would duplicate output.
> ModelGate instead sends an explicit SSE error, an interruption finish reason, and
> `[DONE]`. Around that boundary it also reserves token budget in Redis, reconciles
> against provider usage, and writes a redacted PostgreSQL audit trail. The behavior is
> covered by 231 tests, including backpressure and packet-boundary failures.

The demo exits non-zero if either guarantee breaks. It uses local mock providers and does
not need the Groq key. Run `docker compose down` afterward.

## Ten-minute explanation

### 0:00-1:00 - Problem and scope

An application should not lose its LLM feature because one provider returns 429, hangs,
or disconnects. ModelGate gives that application one OpenAI-compatible endpoint, ordered
provider failover, tenant token budgets, a short-lived exact cache, and an audit trail.
It deliberately excludes billing, a dashboard, semantic caching, and public deployment.

### 1:00-3:00 - The commit boundary

The first content byte is the central design boundary. Before it, provider metadata can
be buffered and discarded, so another provider can take over without changing what the
caller sees. After it, the response is committed. Starting again would repeat text and a
second model cannot recover the first model's sampling state. ModelGate therefore retries
only before commitment and terminates explicitly after it.

### 3:00-5:00 - Streaming correctness

The relay parses SSE across arbitrary TCP chunks, preserves valid frames, and respects
downstream backpressure instead of reading an unbounded response into memory. It also
distinguishes a normal terminal frame from a clean socket close, a truncated frame, a
provider error frame, a timeout, and a caller disconnect. Once committed, every gateway
failure ends with an error event, `finish_reason: "modelgate_interrupted"`, and `[DONE]`.

### 5:00-7:00 - Token budgets and accounting

Admission reserves prompt tokens plus the requested completion ceiling in a Redis token
bucket before provider dispatch. Completion reconciles that reservation with actual
usage. During a stream, a cheap running length is the runaway tripwire, while the complete
prefix is re-encoded periodically because provider deltas do not reliably align with
token boundaries. Provider usage replaces the estimate whenever it arrives. Interrupted
streams are recorded as `partial_estimated` rather than pretending the estimate is exact.

### 7:00-8:30 - Audit and privacy choices

PostgreSQL records the request and every provider attempt, including provider request IDs,
commit state, bytes sent, token counts, and failure details. Prompts and completions are
redacted before storage. Redis keeps raw cached output only briefly and with persistence
disabled, because returning a cached response requires the original text. That tradeoff is
documented instead of calling the system fully privacy-safe.

### 8:30-9:30 - Evidence

The suite has 231 unit and integration tests. It includes TCP-level mocks for hangs,
rate limits, truncated frames, clean closes, socket resets at selected offsets, and a
downstream backpressure test. In induced-failure benchmarks, 100 pre-commit failovers
matched the backup exactly and 50 post-commit failures all ended explicitly. A 200-request
paired loopback run measured 11.62 ms p99 added gateway overhead; it is a local systems
measurement, not a claim about internet or model latency.

The opt-in Groq validator also passed 26 requests with provider-reported usage. A measured
model-specific prompt offset reduced the residual estimate error to -2 through +1 tokens.

### 9:30-10:00 - Tradeoff and next step

Cross-provider continuation is not enabled. A controlled Groq prefill test reproduced an
exact supplied prefix, but that does not prove an open-ended answer will continue without
a semantic seam. The honest behavior is explicit interruption until a live evaluation can
show that continuation is better than stopping. If the project grew beyond a portfolio
scope, the next work would be multi-tenant authentication, usage export, and a deployed
load test rather than another provider adapter.

## Short answers to likely questions

**Why not retry after output starts?** The caller has already observed state. A replay can
duplicate text, and a different provider cannot reproduce the original sampling state.

**Why reserve the maximum token cost?** Admission based only on the prompt permits many
large completions at once. Reserve the worst case, then refund the difference.

**Why re-encode the full prefix?** Counting each delta separately is correct only when the
provider happens to split text on tokenizer boundaries.

**Why keep both Redis and PostgreSQL?** Redis handles short-lived atomic budget and cache
operations; PostgreSQL holds durable relational audit history.

**What was the hardest bug?** Stopping the read loop mid-chunk left unread frames in the
framer. Flushing that buffer leaked the rest of the packet to the caller after the token
tripwire fired, so delivery and accounting disagreed. The fix discards an incomplete tail
on interruption, and a split-delta integration test prevents regression.

**What would you change for production?** Add real tenant identity and key rotation,
external usage export, broader load and fault testing, and provider-specific operational
limits. Keep the commit-boundary rule unless continuation earns its way in with evidence.
