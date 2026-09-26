# Halfwave: AI-Infra Adoption Radar

Tracks ~25 AI-infrastructure projects (inference engines, serving frameworks, local runtimes,
LLM gateways) and asks one question: **does rising attention translate into observable adoption?**
Every derived signal traces back to its source records.

```
sources → Python producers → Kafka → Bronze (Structured Streaming) → Silver (SDP)
        → Gold (dbt) → MongoDB → FastAPI
```

**Status:** work in progress. See [docs/SPEC.md](docs/SPEC.md) for scope and timeline, and
[docs/decisions.md](docs/decisions.md) for the reasoning behind each design choice.
