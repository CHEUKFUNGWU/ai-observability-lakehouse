# ADR 005: Why The Agent Model Is Generic

## Context

The project targets observability patterns across frameworks, not only Dify workflow internals.

## Decision

Model Agent runs, spans, and tool calls as generic runtime facts.

## Consequences

- The schema works across multiple agent frameworks.
- Mapping from framework-specific events requires adapters.
- Portfolio value improves because the design is not tied to one vendor runtime.
