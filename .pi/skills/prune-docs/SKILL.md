---
name: prune-docs
description: Audit and prune Helix documentation while accounting for retained and removed guidance.
disable-model-invocation: true
---

# Prune documentation

Before any writing or writing-style audit in this workflow, every participating agent reads and applies the `writing-for-agents` skill; it owns the writing rules. Read this repository's `AGENTS.md` for documentation ownership.

## 1. Audit

The main agent audits before delegating rewrites. Use the requested scope; otherwise include the root README, architecture, agent instructions, module guides and their linked references. Record the baseline revision and any uncommitted changes, then read every scoped document completely. Consult earlier rewrites when they explain accumulated material.

For each document, identify its reader's task and the passages that obstruct it. Distinguish current constraints and non-obvious rationale from historical evidence, duplication and cheap source lookups. Verify suspected stale claims against current source; keep unresolved claims explicitly uncertain.

## 2. Settle

Freeze findings into a closed FIX/WONTFIX docket with passages, reasons and intended authoritative homes. FIX only changes that lower complexity. Track bounded FIX scopes in `bd`, with acceptance criteria; keep the docket and work notes in issues or temporary files outside the repository. For audit-only requests, stop here.

Before rewriting, account for every substantive instruction, constraint and caveat in the affected passages: retain it, replace it with a reachable source pointer, or drop it with a reason. Include prerequisites, trading restrictions, recovery procedures and verification limits. An old probe may support a current warning: discard the transcript, preserve the warning. A reduction target is not evidence that material is expendable.

## 3. Rewrite

Delegate non-overlapping document scopes to subagents using the requested model when available. Give each the writing skill, baseline, FIX docket, retention decisions, owned paths and required discovery/coverage evidence. Workers read their scoped documents fully and change only accepted findings; the main agent owns cross-file integration.

Each meaning has one home. Split only for a distinct reader task and add a trigger-bearing pointer. Keep examples tied to maintained configs and record experiment history in issues rather than creating a documentation archive. Runtime changes are separate work.

## 4. Verify retention

The main agent compares each rewrite with the baseline, including changes made during integration. Every changed or removed substantive statement must have a verified destination or a justified deletion in the retention record. Follow replacement pointers: an existing file alone does not prove that the reader can recover the needed meaning. Restore lost constraints and uncertainty before accepting the rewrite.

Check relative links and anchors, example/config consistency, snippet syntax and the final diff. Real orders, token rotation, paid requests and destructive resets require explicit authorization; syntax checks alone establish no runtime correctness. Classify any review findings in a closed FIX/WONTFIX docket before correcting them.

Finish when every scoped document and retention decision is accounted for and the checks pass. Close completed issues and report scope, intentional removals, checks and unresolved uncertainty. Word counts describe the result, not its quality.
