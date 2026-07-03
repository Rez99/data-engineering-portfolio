# 1. Project Brief

## Table of Contents

- [1. Project Brief](#1-project-brief)
  - [1.1 Context](#11-context)
  - [1.2 Vision](#12-vision)
  - [1.3 Constraints](#13-constraints)
- [2. Project Proposal](#2-project-proposal)
  - [2.1 Directory structure](#21-directory-structure)
  - [2.2 Data pipeline](#22-data-pipeline)
  - [2.3 Serving Layer](#23-serving-layer)
    - [2.3.1 Portfolio view (all projects)](#231-portfolio-view-all-projects)
    - [2.3.2 Project snapshot](#232-project-snapshot)
    - [2.3.3 Project Kanban](#233-project-kanban)

## 1.1 Context

I work for an engineering company where I manage multiple concurrent client engagements. Some projects last a few weeks while others span many months.

Project information is fragmented across many sources:

- Email conversations
- PDF reports and deliverables
- Meeting notes and call transcripts
- Internal documents
- Project plans
- Other unstructured documents

As a result, it is difficult to quickly understand the current state of every engagement.

## 1.2 Vision

I want a system where I can open my laptop on Monday morning and immediately see the status of my entire client portfolio.

For each client I would like to know:
- Current stage of the engagement
- Overall health (for example, green / amber / red)
- Current blockers
- Next action required
- Whether the next move is ours or the client's
- Upcoming milestones
- A short timeline of recent events
- The ability to drill into supporting evidence if required

The goal is not simply reporting—it is creating an AI-assisted engagement operating system that continuously synthesizes information from many sources into a single portfolio view.

## 1.3 Constraints
- I have approximately one week to build a proof of concept.
- I will present the idea at a company retreat in three weeks.
- The objective is to demonstrate value, not perform a full migration of company data.

# 2. Project Proposal
## 2.1 Directory structure
The foundation of the system is deceptively simple: files and folders.

Rather than leaving client information scattered across emails, shared drives, CRMs, and meeting platforms, every client engagement is consolidated into a **single repository**. This creates one canonical workspace where both humans and AI agents can find and work with the complete history of each project.
```text
client-engagement/
├── client_a/
├── client_b/
├── client_c/
|   ├── project-alpha/
|   ├── project-beta/
|       ├── bronze/
|       ├── silver/
|       └── gold/
|
├── client_d/
├── client_e/
```
## 2.2 Data pipeline
Each project progresses through a three-stage pipeline, where information is progressively enriched and distilled into more useful forms.
* ***📥 Source*** – Continuously ingest artifacts from external systems (email, Teams, Slack, SharePoint, CRMs, etc.) using APIs and automation scripts running on a scheduled cadence.
* ***🥉 Bronze Layer*** – Preserve the original source artifacts (emails, PDFs, transcripts, documents, etc.).
* ***🥈 Silver Layer*** – Normalize every artifact into a common representation *(Markdown)*, providing a consistent, AI-friendly format regardless of the original file type.
* ***🥇 Gold Layer*** – Transform normalized content ***via AI*** into structured knowledge *(JSON)* that can be queried, reasoned over, and used to generate project snapshots, timelines, and dashboards.
* ***📊 Serving Layer*** – Present the latest engagement state through intuitive views such as portfolio dashboards, project snapshots, timelines, Kanban boards, and AI-powered search and chat.

```text
Source data
--------------------------------
               
               ⬇

Bronze Layer (original artifacts)
--------------------------------
Emails
Call Transcripts
Slack messages
PDFs
Documents

               ⬇

Silver Layer (normalized artifacts)
--------------------------------
*.md

               ⬇ (🤖 AI)

Gold Layer (structured data)
--------------------------------
*.json

               ⬇

Serving Layer (visual)
--------------------------------
Snapshots
Timelines
Dashboards
Kanban
```
Example of structured data (`.json`) extracted by the LLM (Gold Layer):
```json
{
  "client": "Acme Corp",
  "project": "Project Alpha",
  "artifact_type": "Email",
  "timestamp": "2026-07-10T09:15:00Z",

  "summary": "Customer reviewed the proposal and requested pricing revisions.",
  "stage": "Proposal",
  "events": ["Proposal reviewed", "Pricing revisions requested"],
  "actions": ["Vendor to revise pricing", "Schedule follow-up meeting"],
  "risks": ["Project approval delayed pending pricing changes"],
  "decisions": ["Proceed with revised proposal"],
  "stakeholders": ["John Smith (Engineering Manager)", "Jane Doe (Project Lead)"]
}
```

## 2.3 Serving Layer
### 2.3.1 Portfolio view (all projects)
```text
+--------------------------------------------------------------------------------------------------------------+
| Client         | Stage          | Health | Waiting On         | Next Milestone         | Last Updated       |
+----------------+----------------+--------+--------------------+------------------------+--------------------+
| Acme Corp      | Proposal       | 🟡     | Client Feedback    | Proposal Due Jul 18    | Jul 10             |
| Globex Inc     | Discovery      | 🟢     | -                  | Discovery Call Jul 14  | Jul 10             |
| Initech LLC    | Delivery       | 🔴     | Vendor             | UAT Complete           | Jul 10             |
| Soylent Co     | On Hold        | 🟡     | Internal Review    | Decision Jul 25        | Jul 09             |
| Umbrella Corp  | Kickoff        | 🟢     | -                  | Kickoff Jul 11         | Jul 10             |
+--------------------------------------------------------------------------------------------------------------+

Legend: 🟢 On Track   🟡 At Risk   🔴 Blocked
```
### 2.3.2 Project snapshot
```text


+--------------------------------------------------------------------------------------+
| Project : Acme Corp          Stage : Proposal          Health : 🟡 At Risk            |
+--------------------------------------------------------------------------------------+

Summary
-------
Proposal has been submitted. Customer requested pricing and scope revisions.
Awaiting client feedback before progressing to implementation.

Open Actions
------------
[ ] Send revised pricing model
[ ] Schedule follow-up call
[ ] Update implementation timeline

Current Risks
-------------
🟡 Waiting on client approval
🟢 Technical feasibility confirmed
🟢 Budget approved

Recent Activity
---------------
Jul 10  Customer requested pricing revisions
Jul 09  Revised proposal sent
Jul 05  Proposal submitted
Jul 02  Kickoff meeting completed

Top Documents
-------------
• Proposal_v2.pdf
• Kickoff_Notes.md
• Pricing_Model.xlsx
• Meeting_Transcript_2026-07-10.md
```
### 2.3.3 Project Kanban
```text
+------------------+------------------+------------------+------------------+
| Backlog          | In Progress      | Waiting On       | Done             |
+------------------+------------------+------------------+------------------+
| ☐ Exec Summary   | ☐ Revise Scope   | ☐ Client Review  | ☑ Kickoff        |
| ☐ Budget Review  | ☐ Pricing Model  | ☐ Legal Approval | ☑ Proposal V1    |
|                  |                  |                  | ☑ Discovery      |
|                  |                  |                  | ☑ Requirements   |
+------------------+------------------+------------------+------------------+
```
