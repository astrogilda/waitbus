# Strategy — Long-Term Vision and Open-Core Outlook

*Strategic vision document, originally framed 2026-05-16; carried forward
through the v0.5.0 launch-articles cut and into the public repo at the
v0.6.0 wire-stable public-flip launch.*

It records what waitbus is trying to
become, what it is deliberately not trying to become, and the
reasons for both. It is grounded in code-verified findings about the
ecosystem (MCP standardization under the Linux Foundation, Anthropic's
Channels work, LangGraph's `interrupt()`, hosted-relay incumbents like ngrok
Agent Endpoints and Cloudflare managed MCP).

---

## Executive summary

waitbus is the workstation-local, cross-harness status bus: a small daemon and a
`wait`/`emit` primitive that lets any script or agent on your machine sleep
until any local-or-remote thing it cares about finishes or fails, without
polling — and wakes every other tool on the box at the same moment. The
long-term vision is to be the **local async nervous system for your own
machine's agents**: framework-neutral, offline-capable, multi-source,
MIT-licensed, and quietly depended on.

The realistic best outcome is a beloved free OSS tool in the spirit of
`ripgrep`, `fzf`, or `direnv`. A Sidekiq-shaped open-core outcome is
genuinely possible but strictly downstream of adoption. Venture funding is
rejected as a path for structural reasons.

The core stays MIT, forever. Everything else is downstream of that one rule.

---

## The long-term vision

waitbus stops being "a CI-status pusher for one agent framework" and becomes a
universal local-wait primitive — CI is merely source #1.

Three capabilities flip what the thing actually is:

- **A universal `wait` verb.** A dumb, exit-coded `waitbus wait` usable from
  bash, a Python loop, an editor agent, a graph-based runner. MCP becomes
  *one adapter*, not *the* interface. This removes any single-vendor lock-in.
- **Local event sources.** The `source` substrate already proves the model
  end-to-end with Prometheus / Alertmanager. Extending to pytest, docker,
  inotify, and similar local sources makes "wake me when *the local thing*
  finished" true — and that is precisely the part hosted cloud orchestrators
  structurally cannot do well, because it never leaves the machine.
- **Measurable overhead.** A `stats` command that turns the value claim
  into a number you can show, not assert.

**Example:** an agent kicks a local test run, calls
`waitbus wait --source pytest`, sleeps for zero tokens, and wakes the instant
results land. Same call shape for a docker build, a file change, or CI on a
pushed branch. Identical behavior whether the caller is an editor agent or a
twenty-line bash script.

**The coordination corner.** The same broadcast fan-out that delivers source
events to one agent already delivers any agent's `emit()` to every other agent
on the box — so waitbus is, latently, a same-machine agent-coordination
backplane: one agent emits "claimed `parser.py`" or "build failed, here is the
traceback," and the others wake on it with zero polling. This is surfaced as a
discovery instrument (`waitbus swarm-demo`), not yet a committed product surface:
the heavier *addressed-messaging* fork (a `to:` field, reply threads, handoff
state — the model the agent-message-queue and inter-session projects target) is
held pending evidence of real user demand, because it is a one-way data-model
decision. A 2026-05-28 market verification places multi-agent coordination as
the live developer-tooling category, while confirming the defensible corner —
push of events the agent did *not* initiate, normalized across heterogeneous
sources on a single peer-credential-gated machine — is exactly the capability
the Model Context Protocol itself leaves to "On the Horizon" rather than
shipping.

**Ecosystem position:** not "the standard." A sharp, beloved
**reference implementation of a small primitive the platforms are converging
on** — the tool you reach for on your *own* workstation when you don't want a
cloud orchestrator and don't want vendor lock-in. Durable territory is
exactly the corner the cloud hubs and the agent-framework vendors concede:
**local, private, offline, multi-source, framework-neutral.** Not a moat —
a niche waitbus fits unusually well.

**The defensibility caveat that bounds the vision.** The GitHub-CI path is
the *most* absorbable by hosted platforms. The defensible value is
specifically the **non-CI, local, offline** sources. That is why local event
sources are not optional polish — they *are* the defensible part. Capability
makes waitbus *able* to be this; it does not make it *be* this. The remaining
gap is **adoption** — a community and communications effort, not a coding
one.

---

## The realistic ceiling: the loved-OSS class

The realistic best-case outcome is to join the class of single-purpose local
dev tools that are installed everywhere and quietly depended on, without
being companies. License facts verified 2026-05-16:

| Tool | What it is | License | Company? |
|---|---|---|---|
| **ripgrep** (`rg`) | Faster `grep`; respects `.gitignore`; bundled inside VS Code search | Dual MIT / Unlicense | No company. Individual project; separately employed maintainer; GitHub Sponsors. Corporate *usage*, not ownership. |
| **fzf** | Command-line fuzzy finder | MIT | No company. Individual project; merch and sponsorship. |
| **direnv** | Per-directory env auto-load/unload | MIT | No company *owns* it. A small consultancy (Numtide) sells paid support and services around it. |

The framing: none of these is a company-owned product, but
maintainers commonly monetize **indirectly** — sponsors, merch, or a support
consultancy. The ceiling is "beloved OSS tool where any money is indirect and
optional," not "a paid product."

### What "consulting value" means when the software is free

The Numtide pattern: a small remote consultancy selling commercial support
around OSS that the maintainers also work on. Free code does not make the
*expertise, time, or guarantees* free. Companies pay for what the repo doesn't
give them:

- **Integration into their specific environment** — the binary is free;
  making it work across a 500-engineer org's infrastructure is not.
- **An SLA and a throat to choke** — free OSS has no 2 a.m. phone number; a
  support contract does.
- **The maintainer's brain** — the consultancy employs the people who can
  change the project and upstream the fix.
- **Sponsored features and roadmap influence** — "develop OSS in
  collaboration with companies, split costs."
- **Audits, training, architecture review.**

This is the classic "free software, paid services" model — Red Hat's logic at
consultancy scale. For waitbus the same path exists *if and only if* it earns
real adoption first. It is **lifestyle / small-agency scale, not venture
scale.**

---

## Open-core potential: one clean seam

**Open-core** = free OSS core + a distinct paid edge that *companies* need
and *individuals* don't. For waitbus that line is unusually clean:

- **Free core (MIT, forever):** the local daemon, `wait` verb, local event
  sources, MCP and CLI surfaces — single machine, single user. This is the
  adoption engine; it must stay free.
- **Possible paid edge:** anything that crosses **machines or teams** — a
  hosted zero-config relay plus cross-machine state sync (the thing that
  solves the loopback blindspot), plus team dashboards, audit, SSO, RBAC.
  An individual self-hosts or skips it; a 200-developer company pays to not
  think about it.

**Realistic comparable: Sidekiq.** One person, a free Ruby library that
became ubiquitous, then a closed paid Pro / Enterprise tier on top; a
multi-million-dollar solo business, no venture funding, no team. That is the
*shape* of the best realistic open-core outcome — solo-achievable, proven,
not a fantasy. (Distinct from ripgrep / fzf, which are pure free, and from
the consultancy model.)

### Why a paid relay is consistent with rejecting "cloud-relay as the lead play"

Building a hosted relay as the **standalone venture thesis** is overreach
into ngrok / Cloudflare / Anthropic-Channels territory and is rejected.
Offering the same relay as an **optional paid edge layered on a free core
that is already loved** is the textbook open-core move. Same component,
opposite verdict, because the context (lead play vs. monetization edge on
proven adoption) is what changes it.

### Caveats

1. **Strictly downstream of adoption.** Sidekiq worked because the free gem
   was everywhere *first*, for years.
2. **The platform headwind moves, it does not vanish.** The free core is
   *more* defensible than any paid edge — a paid relay competes with the
   exact incumbents already named. Open-core relocates the fight to the tier
   where waitbus has *least* advantage.
3. **Solo open-core is a real job.** Two codebases, a license boundary,
   billing, support SLAs. Sidekiq is the *top* of that distribution, not the
   median; most solo open-core nets modest side income.
4. **The license seam must be designed early.** Core stays MIT; any paid
   bits stay closed or source-available. Retrofitting that line onto an
   already-public codebase is painful and can poison community trust. If
   open-core is even a *maybe*, keep any team-tier layer **architecturally
   separable from day one** — a free decision now, an expensive one later.

### Outcome ladder

1. **Loved free OSS tool** — most likely good outcome; the actual win
   available.
2. **+ light consultancy / support income** — modest.
3. **Open-core, Sidekiq-shaped** — genuinely possible; *better aspirational
   target than venture* because it is solo-real and adoption-gated; years
   out and contingent.
4. **Venture** — no. Platform absorption plus commodity-layer dynamics make
   this the wrong shape for funded growth.

Open-core is the right thing to **preserve optionality for** (via
architectural separability), **not to plan around**.

---

## Why waitbus does not seek venture funding

This is a deliberate refusal, not a default.

- **Platform absorption.** The most absorbable surface (CI status forwarding)
  is already being absorbed by the agent-framework vendors themselves. A
  venture pitch built on that surface is a pitch built on a shrinking
  territory.
- **Commodity layer.** Hosted relay, tunnel, and webhook-fan-out are
  competitive niches already occupied by infrastructure incumbents with
  enormous distribution and free tiers.
- **Adoption shape.** The defensible value is small, local, single-user, and
  beloved. That shape does not need — and is actively harmed by — the growth
  rate venture capital must underwrite.
- **Outcome geometry.** The realistic ceiling (loved OSS, with
  optional consulting or a Sidekiq-shaped paid edge years out) is a great
  individual outcome and a bad venture outcome. Trying to force it into the
  venture shape destroys the thing that makes it work.

---

## Non-goals

Every drift in any of these directions is killed by the same observations
about ecosystem direction (platform standardization, adjacent positioning
already taken, hosted-relay incumbents).

- **Not a hypervisor.** Not trying to virtualize or sandbox agents.
- **Not a declared standard.** Not racing MCP, Channels, or any
  framework-vendor protocol. A reference implementation, not a spec body.
- **Not a funded company.** See above.
- **Not a cloud-relay business.** A paid relay can only exist as an optional
  edge on a loved free core, not as the lead play.

The vision is durable *because* it stays small and local.

---

## Binding rules

1. **The core stays MIT, forever.** Local daemon, `wait` verb, local event
   sources, MCP and CLI surfaces.
2. **Keep any team / cross-machine layer architecturally separable from day
   one.** Even if nobody ever ships a paid edge, this is the cheap-now /
   expensive-later decision.
3. **Adoption first, monetization options later.** Every roadmap item is
   evaluated against "does this make the free core more loved?" before any
   other criterion.
4. **No venture.** Not now, not at adoption inflection, not at any size.
