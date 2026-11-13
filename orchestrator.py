import asyncio
import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
import aiohttp
from pydantic import BaseModel, Field, ValidationError

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)
logger = logging.getLogger("MultiAgentOrchestrator")


# --- 1. Strict Schema Definitions (Pydantic) ---
class AgentTaskInput(BaseModel):
    task_id: str = Field(..., description="Unique identifier for the workflow task")
    prompt: str = Field(..., min_length=5, description="Input instruction for the agent")
    context_data: Dict[str, Any] = Field(default_factory=dict)
    max_retries: int = Field(default=3, ge=1, le=5)


class StructuredAgentOutput(BaseModel):
    task_id: str
    status: str = Field(..., pattern="^(SUCCESS|PARTIAL|FAILED)$")
    reasoning_summary: str
    extracted_entities: List[str] = Field(default_factory=list)
    action_payload: Dict[str, Any]
    confidence_score: float = Field(..., ge=0.0, le=1.0)
    timestamp_utc: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


# --- 2. Core Asynchronous Multi-Agent Orchestrator ---
class AsyncLLMOrchestrator:
    """
    Production-grade asynchronous orchestrator for multi-agent LLM workflows.
    Features:
    - Strict Pydantic JSON schema validation
    - Exponential backoff & fault-tolerant retry surgery
    - Asynchronous webhook dispatching
    """

    def __init__(self, api_key: str, webhook_url: Optional[str] = None):
        self.api_key = api_key
        self.webhook_url = webhook_url
        self._semaphore = asyncio.Semaphore(10)  # Concurrency rate-limiter

    async def _call_llm_agent(
        self, session: aiohttp.ClientSession, task: AgentTaskInput, attempt: int
    ) -> Dict[str, Any]:
        """Simulates or executes an async LLM API call with structured JSON output."""
        await asyncio.sleep(0.4)  # Simulated network latency

        # Deterministic structured response simulation for architecture showcase
        return {
            "task_id": task.task_id,
            "status": "SUCCESS",
            "reasoning_summary": f"Processed instruction via Planner & Executor agents on attempt {attempt}.",
            "extracted_entities": ["API_Pipeline", "Webhook_Trigger", "JSON_Schema"],
            "action_payload": {
                "routed_to": "crm_integration_service",
                "priority": "HIGH",
                "metadata": task.context_data,
            },
            "confidence_score": 0.96,
        }

    async def execute_workflow_with_retry(
        self, session: aiohttp.ClientSession, task: AgentTaskInput
    ) -> Optional[StructuredAgentOutput]:
        """Executes agent workflow with exponential backoff and Pydantic validation."""
        async with self._semaphore:
            for attempt in range(1, task.max_retries + 1):
                try:
                    logger.info(f"[{task.task_id}] Executing agent workflow (Attempt {attempt}/{task.max_retries})")
                    raw_response = await self._call_llm_agent(session, task, attempt)

                    # Validate output strictly against Pydantic schema
                    validated_output = StructuredAgentOutput(**raw_response)
                    logger.info(
                        f"[{task.task_id}] Schema validation passed (Confidence: {validated_output.confidence_score})"
                    )

                    if self.webhook_url:
                        await self._dispatch_webhook(session, validated_output)

                    return validated_output

                except (ValidationError, aiohttp.ClientError, asyncio.TimeoutError) as err:
                    wait_time = 2 ** (attempt - 1)
                    logger.warning(
                        f"[{task.task_id}] Workflow error on attempt {attempt}: {err}. Retrying in {wait_time}s..."
                    )
                    if attempt == task.max_retries:
                        logger.error(f"[{task.task_id}] Max retries exhausted. Moving task to Dead-Letter Queue.")
                        return None
                    await asyncio.sleep(wait_time)

    async def _dispatch_webhook(
        self, session: aiohttp.ClientSession, payload: StructuredAgentOutput
    ) -> None:
        """Dispatches validated agent output to downstream webhook endpoints."""
        try:
            logger.info(f"[{payload.task_id}] Dispatching validated payload to webhook: {self.webhook_url}")
            # In live production: await session.post(self.webhook_url, json=payload.model_dump())
            await asyncio.sleep(0.1)
        except Exception as exc:
            logger.error(f"[{payload.task_id}] Webhook delivery failed: {exc}")

    async def run_batch(self, tasks: List[AgentTaskInput]) -> List[StructuredAgentOutput]:
        """Runs multiple agent workflows concurrently using asyncio.gather."""
        async with aiohttp.ClientSession() as session:
            coroutines = [self.execute_workflow_with_retry(session, t) for t in tasks]
            results = await asyncio.gather(*coroutines)
            return [res for res in results if res is not None]


# --- 3. Example Execution Entrypoint ---
async def main():
    orchestrator = AsyncLLMOrchestrator(
        api_key="sk-sanitized-production-key",
        webhook_url="https://api.enterprise-client.internal/v1/webhooks/agent-events",
    )

    sample_tasks = [
        AgentTaskInput(
            task_id="WF-1001",
            prompt="Analyze incoming SaaS lead and extract routing entities.",
            context_data={"source": "stripe_webhook", "plan": "enterprise"},
        ),
        AgentTaskInput(
            task_id="WF-1002",
            prompt="Summarize customer ticket and trigger automated escalation.",
            context_data={"source": "zendesk_api", "urgency": "critical"},
        ),
    ]

    completed = await orchestrator.run_batch(sample_tasks)
    print("\n--- Final Validated Workflow Outputs ---")
    for item in completed:
        print(json.dumps(item.model_dump(), indent=2))


if __name__ == "__main__":
    asyncio.run(main())
