import os
import sys

# Add the project root to sys.path so it can find the new package structure
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import streamlit as st
import traceback

# 1. Page Config MUST be the first Streamlit command
st.set_page_config(
    page_title="Institutional Intelligence Platform",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 2. Execute app/main.py directly on every Streamlit rerun.
#    Using exec() instead of import because Python caches module imports —
#    a plain `import app.main` only runs the module code once, causing a
#    black screen on every subsequent user interaction.
_main_path = os.path.join(PROJECT_ROOT, "app", "main.py")
try:
    with open(_main_path, "r", encoding="utf-8") as _f:
        exec(compile(_f.read(), _main_path, "exec"))
except Exception as e:
    st.error(f"## 🛠️ Fatal System Initialization Error\n\nThe platform failed to start due to an internal error.")
    st.warning(f"**Error Details:** `{type(e).__name__}: {str(e)}`")
    with st.expander("🔍 Diagnostic Traceback for Engineering"):
        st.code(traceback.format_exc())
    st.info("Please check the terminal logs for more information.")
