"""
Agora Terminal - AI Query Engine Router
POST /api/ai/query  -- NL to SQL queries
POST /api/ai/chat   -- Conversational text responses
"""
from fastapi import APIRouter
from pydantic import BaseModel
from .engine import run_query, run_chat

router = APIRouter(prefix="/api/ai", tags=["AI Query Engine"])


class QueryRequest(BaseModel):
    question: str
    context: str = ""


@router.post("/query")
def ai_query(request: QueryRequest):
    if not request.question or not request.question.strip():
        return {
            "question": request.question,
            "sql": None,
            "results": [],
            "row_count": 0,
            "duration_ms": 0,
            "error": "Question cannot be empty.",
        }
    return run_query(request.question.strip())


@router.post("/chat")
def ai_chat(request: QueryRequest):
    if not request.question or not request.question.strip():
        return {"answer": "Question cannot be empty."}
    return run_chat(request.question.strip(), request.context)


@router.get("/health")
def ai_health():
    import urllib.request
    import json
    try:
        resp = urllib.request.urlopen(
            "http://host.docker.internal:11434/api/tags", timeout=5
        )
        data = json.loads(resp.read().decode())
        models = [m["name"] for m in data.get("models", [])]
        return {"status": "ok", "ollama": "reachable", "models": models}
    except Exception as e:
        return {"status": "degraded", "ollama": "unreachable", "error": str(e)}