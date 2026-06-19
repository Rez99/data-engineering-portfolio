# AGENTS.md

## Purpose

This repository contains Project 2 of a data engineering portfolio. The objective is to demonstrate the migration of a local lakehouse architecture to a cloud-native deployment using infrastructure-as-code and managed services.

This project is a direct evolution of **Project 1: Local Lakehouse**. Where possible, architectural patterns, pipeline logic, and design decisions should remain conceptually consistent with Project 1, while adapting them appropriately for a cloud environment.

Before beginning any work, read `docs/PROJECT_2_SPEC.md` to understand the project goals, milestones, and acceptance criteria.

---

## Task Planning Workflow

For any non-trivial request, do **not** immediately begin implementation.

Before making code or infrastructure changes:

1. Restate your understanding of the request.
2. Identify any assumptions or ambiguities.
3. Determine which project milestone the work supports.
4. Propose the **smallest independently verifiable implementation task** that advances that milestone.
5. Define clear acceptance criteria for that task.
6. Present the proposed task and acceptance criteria and **wait for approval** before making changes.

---

## Scope and Quota Control

For every implementation request:

1. Identify the smallest change that satisfies the request.
2. List the files expected to change.
3. Separate required work from optional improvements.
4. State whether the task requires an image build, cloud deployment, broad test
   suite, or other substantial autonomous work.
5. Do not perform optional improvements without explicit approval.
6. Stop when the approved acceptance criteria are met.

Before any task likely to involve image rebuilds, cloud deployment, broad test
suites, or substantial autonomous work:

1. Pause before running those operations.
2. Explain why they are necessary.
3. Estimate the files, commands, cloud resources, verification, and teardown
   involved.
4. Wait for explicit approval.

Explicit approval is also required before:

* Changing a component's responsibilities or an architectural decision.
* Adding validation or other behavior to an existing runtime path.
* Provisioning billable cloud resources.
* Updating unrelated documentation or refactoring unrelated code.

---

## Implementation Rules

* Implement only the approved task.
* Keep changes as small and reviewable as practical.
* Do not modify unrelated files or perform opportunistic refactors.
* Explain significant architectural decisions before implementation.
* Avoid introducing new technologies unless they clearly support the project goals.

---

## Test Selection

Use the narrowest test that verifies the changed behavior:

* Documentation-only changes: inspect the diff and run formatting or lint checks
  only when relevant.
* Terraform-only changes: run `terraform fmt` and `terraform validate`.
* A single Python module: run that module's targeted tests.
* Container packaging changes: build only the affected image.
* Deployment or IAM changes: run one focused cloud smoke test.

Do not rerun an unchanged test suite unless dependencies, the execution
environment, or a shared interface changed. Reuse recent successful evidence
when it remains applicable.

Run full regression tests only when:

* A shared contract or cross-component workflow changes.
* Dependencies or base images change.
* Multiple pipeline stages are affected.
* A milestone or release checkpoint explicitly requires them.

Cloud smoke tests, image builds, and full regression tests require the approval
described in **Scope and Quota Control**.

---

## Levels of Completion

Report completion using the most accurate level:

* **Implemented:** The requested files were changed, but execution was not
  performed.
* **Locally verified:** Targeted local checks passed.
* **Cloud verified:** The relevant deployed workflow passed a focused cloud
  smoke test.

Do not silently promote a task to a more expensive completion level. State the
proposed level during planning and obtain approval before image builds, cloud
verification, or broad regression testing.

---

## Completion Workflow

After implementation:

1. Summarize what changed.
2. Explain how the user can verify the result.
3. Call out any assumptions, limitations, or follow-up work.
4. Stop and wait for further instructions before beginning the next task.
