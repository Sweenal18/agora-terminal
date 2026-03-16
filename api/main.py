"""
Agora Terminal — FastAPI Backend
Serves real-time financial data to the dashboard
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routes import crypto, equity, macro

app = FastAPI(
    title="Agora Terminal API",
    description="Real-time financial intelligence API",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(crypto.router, prefix="/api/crypto", tags=["crypto"])
app.include_router(equity.router, prefix="/api/equity", tags=["equity"])
app.include_router(macro.router, prefix="/api/macro", tags=["macro"])

@app.get("/health")
def health():
    return {"status": "ok", "service": "agora-terminal-api"}
