import asyncio
import json
from pipelines.reasoning.orchestrator import ResearchOrchestrator
from infrastructure.config import get_supabase

async def run_test():
    sb = get_supabase()
    orch = ResearchOrchestrator(sb)
    print("Starting E2E Orchestration Test...")
    
    def cb(msg):
        print(f"[STATUS] {msg}")
        
    try:
        res = await orch.run("Analyze Microsoft revenue growth and AI strategy", callback=cb, mode="report")
        with open("test_output.json", "w") as f:
            json.dump(res, f, indent=2)
        print("Finished successfully. Output saved to test_output.json.")
    except Exception as e:
        print(f"FAILED with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(run_test())
