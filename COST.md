# Cost estimate — AWS Fargate, ap-south-1

Round 1 cost reasoning for the collections voice-agent inference service: what the Appendix C
traffic profile costs to run on Fargate, at two scaling configurations, with a recommendation.

**Rates are live, not quoted.** Every figure below was read from the AWS pricing pages for
**Asia Pacific (Mumbai) / ap-south-1** on 2026-11-11, and each is labelled with its source:

| Component | ap-south-1 rate | Source |
|---|---|---|
| Fargate Linux/x86 vCPU | **$0.04256 / vCPU-hour** | aws.amazon.com/fargate/pricing, region selector = Mumbai |
| Fargate Linux/x86 memory | **$0.004655 / GB-hour** | same page |
| Application Load Balancer | **$0.0239 / ALB-hour** (+ $0.008/LCU-hour) | aws.amazon.com/elasticloadbalancing/pricing, region = Mumbai |
| NAT Gateway | **$0.056 / NAT-gateway-hour** (+ $0.05/GB processed) | aws.amazon.com/vpc/pricing, region = Mumbai |

Appendix D's *supplied reference* rates are ≈$0.046/vCPU-hr and ≈$0.0051/GB-hr. Those are
**8.3% above the live ap-south-1 rates** above, so this document computes on the live rates and
reconciles against Appendix D's figures separately (§4). Hours per month use **730** (the
billing convention: 24 × 365 / 12).

---

## 1. Workload

Appendix C's exact schedule, integrated over a simulated day (SPECS §10; `tools/schedule.py`
derives this from the seven windows rather than transcribing it):

| Sim window | Rate | Minutes | Calls |
|---|---|---|---|
| 00:00–08:00 | 10/min | 480 | 4,800 |
| 08:00–08:15 | linear 10 → 50/min | 15 | 450 |
| 08:15–10:00 | 50/min | 105 | 5,250 |
| 10:00–14:00 | 10/min | 240 | 2,400 |
| 14:00–14:15 | linear 10 → 50/min | 15 | 450 |
| 14:15–16:00 | 50/min | 105 | 5,250 |
| 16:00–24:00 | 10/min | 480 | 4,800 |
| **Day total** | | **1,440** | **23,400** |

- **23,400 calls/day ≈ 702,000 calls/month** (30-day month).
- The day is **20 h at baseline and 4 h in campaign windows** (two 2-hour drives: the 15-minute
  ramps plus the 105-minute holds). That split is what the compute math below uses.
- A call is one synchronous `POST /calls`, which fans out to up to **3 downstream attempts**
  (`MAX_ATTEMPTS=3`). The estimate is sized on *call events*, since the service is CPU-light and
  network-bound on the dependency; the retry multiplier is noted under sensitivity (§5).

---

## 2. Config A — 1 vCPU / 2 GB, scale 1 → 5

Per-task-hour = 1 × $0.04256 + 2 × $0.004655 = **$0.05187**

| Period | Math | Cost/day |
|---|---|---|
| Baseline — 20 h × 1 task | 20 × $0.05187 | $1.0374 |
| Campaign — 4 h × **avg 3 tasks** | 4 × 3 × $0.05187 | $0.6224 |
| **Compute total** | | **$1.6598/day** |

- **Monthly compute ≈ $49.80** (`× 30`).
- Scaling to 5 means the ceiling is 5 tasks; the *average* through a campaign window is the
  number that matters, and 3 is the same assumption Appendix D's worked example uses — chosen so
  the two calculations are comparable rather than because the scheduler promises it.

## 3. Config B — 0.5 vCPU / 1 GB, scale 1 → 10

Per-task-hour = 0.5 × $0.04256 + 1 × $0.004655 = **$0.02593** — almost exactly half of Config A
(0.500 of it; the memory component does not halve, so it is 0.4997, not 0.5 on the nose).

**Assumed average campaign-window task count: 5.** "Scale 1 → 10" is a ceiling, not a steady
state, so this is stated as an assumption and shown as a range rather than presented as precision
it does not have.

| Avg tasks in a 4-hour campaign window | Compute/month |
|---|---|
| 3 (same assumption as Config A) | **$24.90** |
| **5 (this document's assumption)** | **$31.12** |
| 6 | $34.23 |
| 10 (the ceiling, sustained — unrealistic) | $46.68 |

Baseline floor is 20 h × 1 task × $0.02593 = **$0.5186/day ≈ $15.56/month** — the lowest cost of
any option here, and the reason Config B wins when baseline dominates.

## 4. Reconciliation against Appendix D

Appendix D is a *worked example supplied by the challenge*, not a live price list, and it says so
("verify current pricing before finalizing"). It is reproduced here exactly and then compared.

**Appendix D's Config A, on its own supplied rates** ($0.046/vCPU-hr, $0.0051/GB-hr;
$0.0562 per task-hour):

| Period | Appendix D | Our recomputation |
|---|---|---|
| Baseline 20 h × 1 task | ≈ $1.13/day | $1.1240/day |
| Spike 4 h × avg 3 tasks | ≈ $0.68/day | $0.6744/day |
| **Daily** | **≈ $1.81/day** | **$1.7984/day** |
| **Monthly compute** | **≈ $54/month** | **$53.95/month** |

Our arithmetic reproduces Appendix D's Config A line-for-line: **$1.80/day, $54/month**. Appendix
D's ALB (~$18/mo) and NAT (~$40–60/mo) are consistent with live ap-south-1 rates
($0.0239 × 730 = **$17.45/mo**; $0.056 × 730 = **$40.88/mo** for one AZ, plus data processing).

**Where the two diverge is the rate, not the method.** Appendix D's per-vCPU rate is 8.3% above
live Mumbai. On live rates Config A is **$49.80/month compute**, not $54 — and that is the
number this document uses for its recommendation. Both are shown so the reconciliation is checkable
in either direction.

**Appendix D's Config B** gives no numbers, only a claim: "roughly half the per-task cost, double
the task count at peak — lands close to Config A on compute." The arithmetic tests that claim
and **finds it conditional**:

| Config B, avg campaign tasks | Compute/month | vs Config A ($49.80) |
|---|---|---|
| **3** (same task count as A) | **$24.90** | **half** — the claim, if task counts match |
| 5 | $31.12 | 37% cheaper |
| 6 | $34.23 | 31% cheaper |
| 10 (ceiling) | $46.68 | 6% cheaper |

So "lands close to Config A" is true only at the *top* of B's range (avg 8–10 tasks). At equal
task counts, B costs half. The claim is directionally right and quantitatively loose, which is
what a range rather than a single figure is for.

## 5. Fixed overheads and the monthly total

ALB and NAT are **traffic-independent**: they are billed for being provisioned, not for how many
calls arrive. That makes them the dominant term at this workload's size.

| Item | Monthly |
|---|---|
| ALB (1 × $0.0239/hr × 730) | $17.45 |
| NAT Gateway (1 × $0.056/hr × 730) | $40.88 |
| *NAT data processing* ($0.05/GB) | small at this volume — call events are ~1 KB, ≈0.7 GB/month → **≈$0.04** |
| **Fixed overhead subtotal** | **$58.33** |

| Configuration | Compute | Fixed | **Total/month** |
|---|---|---|---|
| **Config A** (1 vCPU/2 GB, 1→5, avg 3) | $49.80 | $58.33 | **$108.12** |
| **Config B** (0.5 vCPU/1 GB, 1→10, avg 5) | $31.12 | $58.33 | **$89.45** |
| Config B at avg 3 | $24.90 | $58.33 | $83.23 |

Sensitivity worth stating plainly: **the fixed overhead is 54–70% of the total.** Halving compute
by choosing Config B saves $18.67/month; adding a second NAT AZ (+$40.88) costs more than both
compute configurations' difference. Cost work at this size is an availability-topology decision
first and a task-size decision second.

## 6. Tradeoffs

**Fewer, bigger tasks (A) vs more, smaller (B).**
- **Scaling granularity.** B steps in half-task increments, so it tracks Appendix C's 15-minute
  ramp more closely: the ramp is a 10 → 50 calls/min swing in 15 minutes, and a configuration that
  can add 0.5 vCPU at a time wastes less idle capacity on the way up and down.
- **Baseline floor.** B's floor is half of A's ($15.56 vs $31.12/month). With 20 of 24 hours at
  baseline, the floor is most of the bill, so this is B's strongest argument.
- **Peak headroom.** A's 5-task ceiling is 5 vCPU; B's 10-task ceiling is also 5 vCPU — *identical
  peak capacity*, reached in two different shapes. B can shape the curve; A cannot.
- **Per-task overheads.** More tasks means more container starts, more ALB target registrations,
  and more per-task memory overhead (each takes a fixed slice for the runtime). At two tasks'
  difference that is noise; at ten it is not. The measurement that would settle it — per-task
  memory floor of this image — has not been taken, so this stays an acknowledged cost of B.

**Fixed overheads that do not scale.** ALB and NAT are the same $58.33/month whether the service
handles 702,000 calls or 7,000. Only LCU charges float with traffic, and at ~1 KB per event they
are negligible against the hourly fee. Any argument that "autoscaling will save money" has to
beat this floor first.

## 7. Recommendation

**Config B (0.5 vCPU / 1 GB, 1 → 10)** — **≈$89/month** total at the stated assumption (avg 5
campaign tasks; $83–$96 across avg 3–6), against **Config A's ≈$108/month**.

The reasoning, in order of weight:

1. **Same peak capacity, half the floor.** Both configurations top out at 5 vCPU. B costs half as
   much to sit at baseline, and baseline is 20 of 24 hours. That is the decision, and it does not
   depend on the campaign-window assumption.
2. **Finer scaling granularity matches the workload's shape.** Appendix C is a steep 15-minute
   ramp into a 105-minute hold, twice a day. Half-vCPU steps fit that better than whole-vCPU
   steps, so less capacity is provisioned but idle.
3. **The risk is bounded and measurable.** B's downside is per-task overhead at 10 tasks — a real
   cost, but one that only bites if the platform actually sits at the ceiling, which the
   campaign-window average says it will not.

**Choose Config A instead if** per-task memory overhead proves significant for this image, or if
the platform needs single-task CPU above 0.5 vCPU for latency headroom. Neither is measured yet;
the honest position is that B is the better default and A is the safer one, and the tiebreaker is
a memory-profile measurement this Round 1 does not include.

**Not included:** multi-AZ NAT (+$40.88/month per additional AZ), data transfer out, CloudWatch
logs/metrics, and Savings Plans (which would cut the compute line by up to ~50% on a 1–3 year
commitment). All are Round 2 concerns; listing them is cheaper than pretending they are zero.
