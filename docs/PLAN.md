# ModelGate build plan

Working plan. Read `CLAUDE.md` first for what the project is and why it exists. This file
only covers how it gets built and what has already been settled.

Full research and citations live in `docs/RESEARCH.md`. That file is a working note, not
prose for a reader.

## Context

ModelGate is a proxy between an app and LLM providers. It forwards requests, fails over
when a provider dies, limits by token cost rather than request count, caches, and keeps an
auditable log of every prompt and completion.

It exists to demonstrate backend systems design for internship applications, which is the
gap the rest of the portfolio does not cover. The bar: runs from a clean clone, one
decision defensible for ten minutes, tests that fail when behaviour breaks, an honest
README.

Before any of it was written, a research sweep checked the assumptions the design rested
on. Three claims went to adversarial reviewers and all three were overturned. They come
first here, because they changed the build.

## What the research overturned

**Assistant-turn prefill cannot be the continuation mechanism.** Every current Claude model
returns 400 with `"This model does not support assistant message prefill"`, confirmed
across five unrelated repos. OpenAI never documented it at all. Groq documents prefill as
output shaping, never as resuming a mid-sentence cut, and nobody has published evidence
either way. Worse, Anthropic's own API reference still says prefill works, so the
capability is not discoverable except by eating a 400.

The portable primitive is a synthetic user turn: *your previous response was interrupted
and ended with X, continue from there*. It needs no provider feature, so it works
everywhere. Prefill becomes a capability flag on a hand-curated table, used only where
verified against a live model.

**Authoritative token usage from a stream is conditional, not a property.** It needs
provider support, the opt-in flag, and a natural stream end. OpenAI documents plainly that
an interrupted or cancelled stream may not deliver the usage chunk. Mid-stream failover and
client abort are exactly those cases. So `token_source: 'estimated'` is the normal path for
the headline feature, not an edge case, and the audit schema has to record which source
every number came from.

**Reserve-then-reconcile is correct under concurrency, not under crashes.** A crash between
reserve and settle refunds tokens the provider genuinely generated, so the budget
under-counts real consumption. That is an accuracy-for-liveness trade and has to be named
as one. Two real bugs turned up in the proposed Lua before a line was written: an over-cap
early return that silently destroys every reclaimed token, and a non-integer `est` that
makes the limiter fail hard instead of open.

The interview answer is *reserve-first eliminates concurrent-admission overshoot exactly;
estimation error and crash-time refunds are the two residual inaccuracies, both bounded by
one refill period*. Not "correct under crashes", which collapses on the first question.

## Phases

Ordered so that stopping after any phase still leaves a shippable project. That property is
the point of the ordering, not an accident of it.

| # | Phase | Done when |
|---|---|---|
| 0 | Foundations. TS + Fastify skeleton, Postgres and Redis via docker compose in WSL2, Zod-validated config, pino logging, Vitest, CI | clean clone, `docker compose up`, `npm test` passes, on steps that were actually run |
| 1 | Mock provider, plus the Groq prefill experiment | every failure mode in `RESEARCH.md §1` is selectable by header and covered by a test |
| 2 | Non-streaming proxy. Adapter and profile, retry, failover, token budget, exact-match cache, audit log | kill provider A mid-demo and the response still arrives from B. Demoable, resume-true |
| 3 | Streaming passthrough. Framer, backpressure, abort, commit point. No token counting yet | byte-at-a-time framer test passes, TTFT at the client within 100ms of the mock's first token |
| 4 | Token accounting through the stream. Three counters, reconciliation, `token_source` | per-delta re-encode is never used, reconciliation matches provider usage on a clean stream |
| 5 | Mid-stream failover. Tier 1, then Tier 3, then Tier 2 timeboxed | zero duplicated characters at the seam, zero silent truncations, both asserted by tests |
| 6 | Measurement, then the honest README | numbers in the README came from a script in the repo |
| 7 | Backpressure proof. A blocked writable stands in for a slow caller | upstream pulls stop before memory grows with the response |
| 8 | Live Groq validation. Prompt accounting, streaming and prefill | dated provider results come from an opt-in script and no secret enters Git |

The brief's stage 2 was one bullet hiding four subsystems, so it split into
phases 0 and 2 here. The Groq prefill experiment was planned for phase 1, but no
key was available then. The deterministic path stayed shippable without it,
and the live experiment ran in phase 8 when a key became available.

### The three failover tiers

**Tier 1, nothing flushed yet.** Clean failover. Provider B gets the original request
verbatim, so there is no seam and no duplication. The commit-point design makes this the
common case, covering connect failures, 5xx, 429 and TTFB timeouts. It ships alone as a
complete feature.

**Tier 3, bytes flushed and continuation impossible.** Terminate honestly with an in-band
SSE error frame and `finish_reason: "modelgate_interrupted"`. About thirty lines. Never
stop silently.

**Tier 2, bytes flushed and continuable.** Best effort. Word-boundary write-behind buffer,
synthetic user turn, overlap dedupe, exactly one attempt. Optional and timeboxed. If it
does not work the README says continuation is not attempted, and the project is still
strong, since that is OpenRouter's published position.

The guarantee to defend is never duplicate, never silently truncate. Not seamlessness. The
second model has no access to the first's sampling state, and no production gateway ships
this: OpenRouter and Portkey decline it by design, Cloudflare's resumable streaming is
same-provider reconnect, and LiteLLM's attempt has five open bugs.

## Decisions already settled

Detail and sources in `RESEARCH.md`.

| Area | Decision |
|---|---|
| Framework | Fastify, with `reply.send(passThrough)` for SSE. Backpressure for free, plus a free client-disconnect signal. Not `hijack`, not bare `raw` |
| HTTP client | `undici.request()` with a per-origin Pool. Not global fetch, because `bodyTimeout` is the inter-chunk idle timer and fetch cannot set it |
| Provider shape | One OpenAI-wire adapter plus a `ProviderProfile` data object. Not two adapters, and not a class hierarchy |
| Tokenizer | `gpt-tokenizer`, subpath imports, loaded eagerly at boot. Not WASM tiktoken, roughly 40× slower for this call pattern. No Llama path, since Llama left the Groq free tier |
| Rate limiting | Token bucket with variable cost, two Lua scripts, ZSET leases pruned on reserve, settled-marker for idempotency |
| Cache | Exact match on a hash of the normalized raw request. No semantic caching |
| Redaction | One choke point behind a branded `Redacted` type, so passing a raw prompt is a type error rather than a code-review catch |
| Ordering trap | Cache key over the raw prompt, audit record over the redacted one. Reversed, two prompts differing only by an email collide and you serve user A's completion to user B |
| Infra | Docker Engine inside WSL2. Already running, no Docker Desktop, and `docker compose up` in the README stays literally true |
| Providers | Mock for deterministic tests and local performance. Groq is an opt-in live validation path. The Azure key stays untouched |
| Dependencies | `libphonenumber-js` and pino's `redact`. Nothing else for redaction, because the alternatives are abandoned, Python, or marketing |

Not building: semantic caching, a reservation sweeper, Redis Functions, reversible PII
tokenization, NER, `@fastify/sse`, `@fastify/reply-from`, `pg_partman`, auth beyond a
static key, billing, public deployment. Each has a paragraph in `RESEARCH.md §9` saying
why. "Here is where this would go and why I have not built it" is the stronger answer.

## Rhythm

Per phase, and the middle two steps are not optional.

1. Plan. Agree scope, with nothing from the playbook loaded.
2. Code. Written plain, no rules and no checklists. This is measured: agents handed the
   backend rules up front shipped 0 of 3 working implementations, unaided agents shipped
   3 of 3.
3. Test. Tests written and run, real output shown, failures included.
4. Review. Playbook checklist, only the sections the diff touches, then fixes applied.
   Then Huzaifa reviews, with `/code-review ultra` where depth is worth paying for. The
   same rules applied after the code exists found 47% of defects against 13% unaided.
5. Commit. Incremental, explaining why. Subject under 50 characters. No AI attribution.

Review ordering is currently playbook first, then Huzaifa. Worth flipping if seeing the raw
mistakes is worth the slower pass.

## Verification

`npm test` runs unit and integration suites driven entirely by the mock provider, so no
network and no keys. `docker compose up` then `npm run demo` kills a provider mid-request
to watch failover. `npm run bench` produces the phase 6 numbers, reproducible from the
repo.

Three tests carry disproportionate weight:

- **Byte-at-a-time framer.** Feed a known body one byte at a time, assert identical output.
  It fails the instant someone optimises the decoder.
- **Grep-the-whole-store redaction.** About thirty fake secrets through the full gateway,
  then assert no fixture value appears in a `pg_dump`, the captured pino stream, or a Redis
  dump. Per-field unit tests structurally cannot catch the leak this does.
- **Two-tenant cache key.** Prompts identical except for an email. Assert the keys differ.

## Measured in phase 4, correcting the research

`RESEARCH.md` says counting each delta separately over-counts by 28% to 287%. Measured here
with `gpt-tokenizer` on `o200k_base`, the truth is conditional:

| Delta shape | per-delta | prefix re-encode | over-count |
|---|---|---|---|
| Word-aligned English | 5 | 5 | 0% |
| Emoji | 8 | 8 | 0% |
| Japanese characters | 8 | 5 | 60% |
| One word split across deltas | 4 | 2 | 100% |

So per-delta counting is not reliably wrong, it is *unreliable*: identical to the correct
answer when a provider happens to chunk on token boundaries, and badly wrong when it does
not. Since a gateway cannot choose how a provider chunks, the prefix re-encode is the only
count that holds either way. That argument is stronger than the one in the research, and it
is the one to give in an interview.

It also had a consequence for the tests. The mock's deltas were word-aligned, so no
end-to-end test could tell a correct counter from a broken one. The mock now has a
`splitDeltas` mode that cuts words in half, and the accounting test runs against it.

## Measured in phase 5: commitment is the failover boundary

Streaming now walks the configured provider order. An HTTP failure, or a stream
that ends before its first content delta, can retry and move to the next
provider. Buffered role and error frames from failed attempts are discarded, so
the caller receives one clean stream from the provider that answered.

Once a content delta commits the HTTP 200 response, the gateway does not retry.
Replaying the request then would duplicate text the caller has already
displayed. The relay emits an SSE error frame, a
`finish_reason: "modelgate_interrupted"` frame and one `[DONE]` marker instead.
The guarantee is zero duplicated output and zero silent truncation.

Tier 2 continuation was not built. Without evidence from a live provider,
synthetic continuation and overlap removal would add a path whose seam quality
cannot be validated. The mock tests prove clean pre-commit failover and honest
post-commit termination; they do not claim that one model can resume another
model's sampling state.

## Measured in phase 6: local overhead is visible

`npm run bench` now builds an isolated local stack and prints the environment,
sample counts and raw measurements as JSON. On 5 September 2026, 200 paired
requests measured 11.62 ms p99 added latency and 5.46 ms p99 cache-hit latency.
Across 100 streaming samples, pre-commit failover reached its first token in
12.72 ms at p99. No failover output differed from the healthy backup, and none
of 50 interrupted streams ended silently.

These are loopback mock measurements on one laptop. They measure gateway code,
local Redis and local PostgreSQL; they say nothing about model generation or
internet latency.

`npm run demo` uses the same isolated approach for a shorter visible check. It
shows one clean pre-commit failover and one explicit post-commit interruption,
then exits non-zero if either guarantee breaks.

## Measured in phase 7: slow readers stop upstream pulls

A controlled writable with a one-byte high-water mark held its first callback
while the mock body offered 5,003 frames. The relay pulled two frames, then
stopped until the sink drained. With the drain wait deliberately removed, it
pulled all 5,003 and the test failed. This proves the relay obeys a writable's
backpressure signal. The route's `PassThrough` remains bounded at 64 KiB, and
the production relay itself did not need to change.

## Measured in phase 8: Groq adds a fixed prompt cost

`npm run validate:groq` sent 26 synthetic requests through the complete
gateway on 6 September 2026 using `openai/gpt-oss-20b`. Non-streaming and
streaming requests completed, every audit row used provider usage, and the key
remained in the ignored `.env` file.

Across five prompt shapes, Groq counted 63 to 66 more input tokens than the
shared estimate. A 64-token adjustment for this exact model reduced the
residual to -2 through +1 tokens, with 0% median and 2.04% maximum absolute
error. No adjustment was inferred for an unmeasured model.

The 20 prefill trials cut the supplied text halfway through a word. Groq
returned the full prefix and the expected remainder every time. That supports
exact overlap removal, but it does not answer whether an open-ended response
would stay coherent across two models. Tier 2 continuation remains out.

## Carried forward from the phase 3 review

**A rate limit now passes through as 429 rather than 502.** Changed during this phase after
reflection, not to make a test pass. 502 says the upstream is broken and invites an alert;
429 with a retry-after says the quota is spent and says when to come back.

## Resolved from the phase 2a review

**One deadline now bounds the whole provider chain.** It is checked before
every dispatch, so per-provider retries cannot multiply the caller's wait
without limit.

**Undici's timeouts are coarse, and this is measured rather than assumed.** With
`headersTimeout` set to 200ms, the first attempt actually failed at roughly 1.2 seconds and
the chain reached the backup at 2.2 seconds. Sub-second values all land near a second. Use
these timeouts for liveness only, never for a tight latency target, and remember it when
phase 3 sets the inter-chunk idle timer.

**The `x-modelgate-provider` header tells callers which provider served them.** Useful for
the demo and for debugging a failover. It also publishes the provider chain to anyone with a
key, which a real deployment would probably not want. Fine here, worth saying out loud.

## Carried forward from the phase 0 review

Found by the playbook checklists, deliberately not built yet.

**Partition maintenance does not exist.** The migration creates the current month and the
two after it. Nothing creates more. A long-lived database eventually routes live traffic
into the DEFAULT partition, and once rows sit there Postgres refuses to create a partition
covering their range, so the repair becomes a data move rather than a DDL. A canary test
asserts named partitions cover at least the next 30 days, so this fails loudly rather than
silently. Clean clones and CI are unaffected because both migrate fresh.

**Cache keys have a version prefix.** A shared Redis outlives a deploy, so new
code must not deserialize a payload written under an older format.

**The Dockerfile, when it exists, must use `CMD ["node", "dist/index.js"]`.** Not npm and
not a shell form. Neither forwards SIGTERM, which would make the graceful shutdown in
`src/index.ts` dead code while looking fine.

**Bring the stack up with `docker compose up -d --wait`.** A bare `up -d` exits 0 as soon as
containers are created, so a service crash-looping on bad config still reports success.

**CI runs on GitHub.** The phase 6 push passed install, typecheck, build and all
tests from a fresh runner.

## Open question

Client disconnect policy is unsettled. Either record `estimated` and abort upstream
immediately, or drain upstream to capture usage. Currently leaning towards aborting:
holding a provider connection open past the client's is a worse property for a gateway than
a slightly drifted number.
