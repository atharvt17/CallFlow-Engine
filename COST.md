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
| NAT Gateway | **$0.056 / NAT-gateway-hour** (+ $0.056/GB processed) | aws.amazon.com/vpc/pricing, region = Mumbai |
| Public IPv4 address | **$0.005 / in-use address-hour** | aws.amazon.com/vpc/pricing (VPC pricing, public IPv4) |

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
Note that Appendix D's overhead figures appear to stop at the two named line items; §5 below adds
public IPv4 and LCU charges that D does not mention, which is one reason our all-in totals run
above D's.

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

| Item | Monthly | Assumption |
|---|---|---|
| ALB hourly (1 × $0.0239/hr × 730) | $17.45 | one ALB |
| **ALB LCU** (see below) | **$0.19** | usage-driven; sized at the 50 calls/min peak |
| NAT Gateway hourly (1 × $0.056/hr × 730) | $40.88 | one NAT gateway, one AZ |
| NAT data processing ($0.056/GB) | **$0.04** | ≈0.7 GB/month at ~2 KB per event |
| **Public IPv4** ($0.005/hr × 2 × 730) | **$7.30** | **2 addresses** — the ALB and the NAT gateway |
| **Fixed overhead subtotal** | **$65.86** | |

**ALB LCU, worked out rather than waved at.** AWS bills LCU-hours in addition to the ALB hourly
fee, at $0.008/LCU-hour. An LCU is the maximum of three normalized dimensions; at this workload's
50 calls/min peak (0.83 req/s, ~2 KB/response, ~300 ms target processing time):

| LCU dimension | Calculation | Value |
|---|---|---|
| New connections/sec | 0.83 / 25 | 0.033 |
| Active connections/min | 25 / 3,000 | 0.008 |
| Processed bytes (GB/hour) | 50 × 2 KB × 60 / 1024³ | 0.0001 |
| **Billed LCU** | max of the above | **0.033** |

At 0.033 LCU → **$0.19/month**. This is negligible, and it is negligible for a checkable reason
(every dimension is three orders of magnitude under its 1-LCU unit), so the sensitivity is: even a
grossly wrong traffic estimate cannot push LCU above a few dollars. An autoscaling target group
that keeps one LCU provisioned regardless would cost $5.84/month — still small, and stated so the
reader can substitute their own assumption.

**Public IPv4 is now included, and it was the largest omission.** AWS charges $0.005 per in-use
public IPv4 address-hour. This stack consumes **two**: the load balancer's and the NAT gateway's.
That is **$7.30/month** — 39% of the A/B compute difference, and 11% of the total overhead. Leaving
it out understates the total by more than the ALB LCU charge would.

| Configuration | Compute | Fixed | **Total/month** |
|---|---|---|---|
| **Config A** (1 vCPU/2 GB, 1→5, avg 3) | $49.80 | $65.86 | **$115.65** |
| **Config B** (0.5 vCPU/1 GB, 1→10, avg 5) | $31.12 | $65.86 | **$96.98** |
| Config B at avg 3 | $24.90 | $65.86 | $90.76 |
| Config B at avg 6 | $34.23 | $65.86 | $100.09 |
| Config B at avg 10 (ceiling) | $46.68 | $65.86 | $112.54 |

Sensitivity worth stating plainly: **the fixed overhead is 57–68% of the total** across the
configurations modelled. Halving compute by choosing Config B saves $18.67/month; adding a second
NAT AZ (+$40.88 hourly, +$3.65 for its public IPv4) costs more than both compute configurations'
difference. Cost work at this size is an availability-topology decision first and a task-size
decision second.

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

**Fixed overheads that do not scale.** The provisioned items — ALB hourly, NAT hourly and the two
public IPv4 addresses — are the same **$65.63/month** whether the service handles 702,000 calls or
7,000. Only LCU and NAT data processing float with traffic, and at ~2 KB per event and 0.033 LCU
they add $0.23 to that floor. Any argument that "autoscaling will save money" has to beat this
floor first.

## 7. Recommendation

**Config B (0.5 vCPU / 1 GB, 1 → 10)** — **≈$97/month** all-in at the stated assumption (avg 5
campaign tasks; $91–$100 across avg 3–6), against **Config A's ≈$116/month**.

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

**The recommendation is robust to the overhead number.** Fixed overhead is $65.86 either way, so
the $18.67/month difference between A and B is purely compute, and the configuration choice does
not depend on getting LCU or IPv4 exactly right. What the overheads *do* change is the framing: at
57–68% of the bill, the platform shape matters less than the topology.

**Choose Config A instead if** per-task memory overhead proves significant for this image, or if
the platform needs single-task CPU above 0.5 vCPU for latency headroom. Neither is measured yet;
the honest position is that B is the better default and A is the safer one, and the tiebreaker is
a memory-profile measurement this Round 1 does not include.

**Not included:** multi-AZ NAT (+$40.88 hourly and +$3.65 in public IPv4 per additional AZ), data
transfer out, CloudWatch logs/metrics, and Savings Plans (which would cut the compute line by up to
~50% on a 1–3 year commitment). All are Round 2 concerns; listing them is cheaper than pretending
they are zero.

**Every number in this document is arithmetic on the rates in the table at the top, 730 hours per
month, and the assumptions stated where each is used.** The three assumptions a reader should check
against their own platform are: the average campaign-window task count (§2, §3), the 0.7 GB/month
NAT data volume (§5), and the two public IPv4 addresses consumed by one ALB plus one NAT gateway
(§5).
