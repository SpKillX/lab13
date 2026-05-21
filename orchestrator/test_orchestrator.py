import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from main import AgentOrchestrator

@pytest.mark.asyncio
async def test_orchestrator_retry_on_timeout():
    orchestrator = AgentOrchestrator()
    orchestrator.nc = AsyncMock()
    
    orchestrator.nc.publish = AsyncMock()

    with pytest.raises(TimeoutError):
        await orchestrator.send_task_with_retry("route_planning", {"test": "data"}, timeout=0.1)
        
    assert orchestrator.nc.publish.call_count == 3