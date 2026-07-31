"""Headless smoke test: runs each module's render() via Streamlit AppTest."""
from streamlit.testing.v1 import AppTest

PAGES = [None, "macro", "direction", "rotation", "kova", "discipline", "proof"]

for page in PAGES:
    try:
        at = AppTest.from_file("app.py", default_timeout=90)
        if page:
            at.query_params["page"] = page
        at.run()
        if at.exception:
            print(f"[FAIL] page={page} -> {type(at.exception).__name__}: {at.exception}")
        else:
            print(f"[ OK ] page={page!s:<9} exception=None")
    except Exception as e:
        print(f"[ERR ] page={page} -> {type(e).__name__}: {e}")
