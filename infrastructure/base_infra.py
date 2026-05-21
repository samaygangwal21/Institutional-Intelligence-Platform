import os
import json
import time
import uuid
import threading
import functools
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, List, Optional, Callable
from loguru import logger

# --- PERSISTENCE & DIRECTORIES ---
METRICS_DIR = ".metrics"
if not os.path.exists(METRICS_DIR): os.makedirs(METRICS_DIR, exist_ok=True)

# --- CACHE ENGINE ---
class CacheEngine:
    """Institutional Caching Layer."""
    def __init__(self):
        self._cache = {}
        self._lock = threading.Lock()

    def cached(self, namespace: str, ttl: int = 300):
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                key = f"{namespace}:{args}:{kwargs}"
                with self._lock:
                    if key in self._cache:
                        val, expiry = self._cache[key]
                        if time.time() < expiry:
                            # logger.debug(f"Cache Hit: {key}")
                            return val
                val = func(*args, **kwargs)
                with self._lock:
                    self._cache[key] = (val, time.time() + ttl)
                return val
            return wrapper
        return decorator

    def invalidate(self, namespace: str):
        with self._lock:
            keys_to_del = [k for k in self._cache.keys() if k.startswith(f"{namespace}:")]
            for k in keys_to_del: del self._cache[k]

cache = CacheEngine()

# --- RESILIENCE ENGINE ---
class ResilienceEngine:
    """Institutional Failure Resilience & Retry logic."""
    @staticmethod
    def with_retry(retries: int = 3, backoff: float = 1.0):
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                last_exc = None
                for i in range(retries):
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        last_exc = e
                        logger.warning(f"Retry {i+1}/{retries} for {func.__name__} failed: {e}")
                        time.sleep(backoff * (2 ** i))
                raise last_exc
            return wrapper
        return decorator

resilience = ResilienceEngine()

# --- OBSERVABILITY ENGINE ---
class ObservabilityEngine:
    """Institutional Observability & Metrics Engine."""
    _metrics = {"start_time": time.time(), "latencies": {}, "error_counts": {}, "cache_stats": {"hits": 0, "misses": 0}}
    _lock = threading.Lock()

    class track_latency:
        def __init__(self, name): self.name = name; self.start = None
        def __enter__(self): self.start = time.time(); return self
        def __exit__(self, *args):
            duration = time.time() - self.start
            with ObservabilityEngine._lock:
                if self.name not in ObservabilityEngine._metrics["latencies"]:
                    ObservabilityEngine._metrics["latencies"][self.name] = []
                ObservabilityEngine._metrics["latencies"][self.name] = (ObservabilityEngine._metrics["latencies"][self.name] + [duration])[-100:]

    @classmethod
    def track_error(cls, source: str, error: Exception):
        with cls._lock: cls._metrics["error_counts"][source] = cls._metrics["error_counts"].get(source, 0) + 1
        logger.error(f"Error tracked from {source}: {error}")

    @classmethod
    def get_summary(cls):
        with cls._lock:
            summary = {"uptime_seconds": int(time.time() - cls._metrics["start_time"]), "average_latencies": {}, "errors": cls._metrics["error_counts"]}
            for name, values in cls._metrics["latencies"].items():
                if values: summary["average_latencies"][name] = round(sum(values) / len(values), 3)
            summary["cache_hit_rate"] = 0 # Placeholder for expanded logic
            return summary

obs = ObservabilityEngine()

# --- WORKER ENGINE ---
class WorkerEngine:
    """Institutional Background Worker Engine."""
    _executor = ThreadPoolExecutor(max_workers=8)
    _jobs: Dict[str, Dict[str, Any]] = {}
    _lock = threading.Lock()

    @classmethod
    def submit(cls, task_fn: Callable, *args, job_name: str, **kwargs) -> str:
        job_id = str(uuid.uuid4())[:8]
        with cls._lock:
            cls._jobs[job_id] = {"id": job_id, "name": job_name, "status": "pending", "start_time": time.time(), "progress": 0, "error": None}
        
        def task_wrapper():
            with cls._lock: cls._jobs[job_id]["status"] = "running"
            try:
                task_fn(*args, **kwargs)
                with cls._lock:
                    cls._jobs[job_id]["status"] = "completed"
                    cls._jobs[job_id]["progress"] = 100
            except Exception as e:
                with cls._lock:
                    cls._jobs[job_id]["status"] = "failed"
                    cls._jobs[job_id]["error"] = str(e)
        
        cls._executor.submit(task_wrapper)
        return job_id

    @classmethod
    def list_jobs(cls):
        with cls._lock: return sorted(cls._jobs.values(), key=lambda x: x["start_time"], reverse=True)[:20]

worker = WorkerEngine()

# --- INSTITUTIONAL MEMORY ENGINE ---
class InstitutionalMemoryEngine:
    """
    Tier 4 AI Workflow Memory Engine.
    Persists research sessions, analytical focus, and strategic conclusions.
    Enables temporal continuity across queries.
    """
    def __init__(self, storage_path="workflow_memory.json"):
        self.storage_path = storage_path
        self.sessions: Dict[str, Dict[str, Any]] = {}
        self.memory = [] # Compatibility with legacy agent memory
        self._load_memory()

    def _load_memory(self):
        try:
            if os.path.exists(self.storage_path):
                with open(self.storage_path, "r") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        self.sessions = data
                    elif isinstance(data, list):
                        self.memory = data
        except Exception as e:
            logger.warning(f"Failed to load workflow memory: {e}")

    def _save_memory(self):
        try:
            with open(self.storage_path, "w") as f:
                json.dump(self.sessions, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save workflow memory: {e}")

    def create_session(self, initial_query: str) -> str:
        session_id = str(uuid.uuid4())[:8]
        self.sessions[session_id] = {
            "id": session_id, "created_at": time.time(), "updated_at": time.time(),
            "initial_query": initial_query, "active_entities": [], "history": [],
            "strategic_conclusions": []
        }
        self._save_memory()
        return session_id

    def add_interaction(self, session_id: str, query: str, response: str, entities_extracted: List[str]):
        if session_id in self.sessions:
            session = self.sessions[session_id]
            session["history"].append({"timestamp": time.time(), "query": query, "response_preview": response[:200]})
            for ent in entities_extracted:
                if ent not in session["active_entities"]: session["active_entities"].append(ent)
            session["updated_at"] = time.time()
            self._save_memory()

    def store_claim(self, topic: str, category: str, claim: str, agent_id: str, confidence: float = 0.5, data: Any = None):
        entry = {"topic": topic, "category": category, "claim": claim, "confidence": confidence, "agent": agent_id, "time": datetime.now().isoformat(), "data": data}
        self.memory.append(entry)

    def get_shared_context(self) -> str:
        if not self.memory: return ""
        ctx = "\n### SHARED ANALYTICAL MEMORY\n"
        for m in self.memory: ctx += f"- [{m['category'].upper()}] {m['topic']}: {m['claim']}\n"
        return ctx

    def get_session_context(self, session_id: str) -> str:
        if session_id not in self.sessions: return ""
        s = self.sessions[session_id]
        ctx = f"--- PRIOR RESEARCH CONTEXT ---\nEntities: {', '.join(s['active_entities'])}\n"
        if s.get("strategic_conclusions"):
            ctx += "Conclusions:\n"
            for c in s["strategic_conclusions"]: ctx += f"- {c['conclusion']}\n"
        for h in s.get("history", [])[-3:]: ctx += f"Q: {h['query']}\nA: {h['response_preview']}\n"
        return ctx

memory_engine = InstitutionalMemoryEngine()
shared_memory = memory_engine
