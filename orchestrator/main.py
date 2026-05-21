import asyncio
import json
import logging
import sys
import uuid
from typing import Dict, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import nats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("orchestrator.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("Orchestrator")

app = FastAPI(title="Store Logistics MAS API")

class RouteRequest(BaseModel):
    order_id: str
    destination: str

class AgentOrchestrator:
    def __init__(self):
        self.nc: Optional[nats.NATS] = None
        self.results: Dict[str, asyncio.Future] = {}
        self.task_counter = 0

    async def connect(self):
        try:
            self.nc = await nats.connect("nats://localhost:4222")
            logger.info("Успешное подключение к NATS.")
            await self.nc.subscribe("tasks.completed", cb=self.on_result)
        except Exception as e:
            logger.error(f"Не удалось подключиться к NATS: {e}")
            raise e

    async def on_result(self, msg):
        try:
            data = json.loads(msg.data.decode())
            task_id = data.get("task_id")
            if task_id in self.results:
                if not self.results[task_id].done():
                    self.results[task_id].set_result(data)
                    logger.info(f"Получен ответ для задачи {task_id}")
        except Exception as e:
            logger.error(f"Ошибка при обработке ответа из NATS: {e}")

    async def send_task_with_retry(self, task_type: str, payload: dict, timeout: int = 3) -> dict:
        task_id = str(uuid.uuid4())
        task = {
            "id": task_id,
            "type": task_type,
            "payload": json.dumps(payload)
        }

        for attempt in range(1, 4):
            logger.info(f"Отправка задачи {task_id}. Попытка {attempt}/3")
            future = asyncio.Future()
            self.results[task_id] = future

            try:
                await self.nc.publish("tasks.process", json.dumps(task).encode())
                result = await asyncio.wait_for(future, timeout=timeout)
                self.task_counter += 1
                logger.info(f"Метрика: Всего успешно выполненных задач оркестратором: {self.task_counter}")
                return result
            except asyncio.TimeoutError:
                logger.warning(f"Таймаут ожидания задачи {task_id} на попытке {attempt}")
                if attempt == 3:
                    if task_id in self.results:
                        del self.results[task_id]
                    logger.error(f"Задача {task_id} провалена после 3 попыток.")
                    raise TimeoutError(f"Превышено время ожидания ответа от агентов для задачи {task_id}")
            finally:
                if task_id in self.results and attempt < 3 and not self.results[task_id].done():
                    del self.results[task_id]

orchestrator = AgentOrchestrator()

@app.on_event("startup")
async def startup_event():
    await orchestrator.connect()

@app.post("/api/v1/route")
async def create_route_task(request: RouteRequest):
    try:
        payload = {"order_id": request.order_id, "destination": request.destination}
        result = await orchestrator.send_task_with_retry("route_planning", payload)
        return {
            "status": "success",
            "agent_output": json.loads(result["output"])
        }
    except TimeoutError as e:
        raise HTTPException(status_code=504, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)