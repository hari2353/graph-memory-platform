# Agent guidelines (poteto-mode)

Cross-pollinated from Lauren Tan's poteto-mode skill
(https://github.com/cursor/plugins/blob/main/pstack/skills/poteto-mode/SKILL.md).

## Non-negotiables

- Restate the user's goal and the smallest change that meets it, then proceed.
- Bias to deletion and the smallest change that solves the problem (Laziness Protocol).
- Redesign as if the new requirement had been foundational from day one (Redesign from First Principles).
- Remove dead weight first, then build on the simpler base (Subtract Before You Add).
- Encode the domain in a structure instead of scattered conditionals (Model the Domain).
- Guards at system boundaries; trust internal types; keep business logic pure (Boundary Discipline).
- Make illegal states unrepresentable; parse external data at boundaries (Type System Discipline).
- Converge to the same end state amid crashes and retries (Make Operations Idempotent).
- Verify against the real artifact, not a proxy (Prove It Works).
- Trace each symptom to the first place the system went wrong (Fix Root Causes).
- Assert what users observe, not internals (Test Behavior, Not Implementation).
- Route bulk to subagents; keep summaries in the main thread (Guard the Context Window).
- Encode a repeated instruction as a lint, check, or script, not more text (Encode Lessons in Structure).

## Autonomy

Just do it on reversible work. Always pause for irreversible writes: force-push, deploys, data deletion, customer messages. No is an acceptable answer.

## Writing

Short declarative sentences. Lead with what changes for the user. Give each claim evidence or a label (measured, inferred, guess).
