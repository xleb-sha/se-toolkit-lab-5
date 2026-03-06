#!/usr/bin/env python
"""Quick test to verify ETL functions are implemented and importable."""

import asyncio
import sys
from pathlib import Path

# Add backend to path
backend_path = Path(__file__).parent / "backend"
sys.path.insert(0, str(backend_path))

async def test_imports():
    """Test that all ETL functions can be imported."""
    try:
        from app.etl import fetch_items, fetch_logs, load_items, load_logs, sync
        print("✓ All ETL functions imported successfully")
        
        # Verify they're callable
        assert callable(fetch_items), "fetch_items not callable"
        assert callable(fetch_logs), "fetch_logs not callable"
        assert callable(load_items), "load_items not callable"
        assert callable(load_logs), "load_logs not callable"
        assert callable(sync), "sync not callable"
        print("✓ All functions are callable")
        
        # Check signatures
        import inspect
        
        sig_items = inspect.signature(fetch_items)
        print(f"  fetch_items{sig_items}")
        
        sig_logs = inspect.signature(fetch_logs)
        print(f"  fetch_logs{sig_logs}")
        
        sig_load_items = inspect.signature(load_items)
        print(f"  load_items{sig_load_items}")
        
        sig_load_logs = inspect.signature(load_logs)
        print(f"  load_logs{sig_load_logs}")
        
        sig_sync = inspect.signature(sync)
        print(f"  sync{sig_sync}")
        
        print("\n✓ Test passed! All functions are properly defined.")
        return True
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    success = await test_imports()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    asyncio.run(main())
