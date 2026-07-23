#!/usr/bin/env python3
"""Build role-sync-demo/deck.html from deck.src.html + assets (base64-inlined)."""

import base64
import pathlib
import sys

HERE = pathlib.Path(__file__).parent
OUT = HERE.parent / "deck.html"
TOKENS = {
    "__SERIF_B64__": "assets/instrument-serif.woff2",
    "__GROTESK_B64__": "assets/space-grotesk.woff2",
    "__MONO_B64__": "assets/roboto-mono.woff2",
    "__LOGO_COLOR_B64__": "assets/logo-color.png",
    "__LOGO_WHITE_B64__": "assets/logo-white.png",
}

html = (HERE / "deck.src.html").read_text()
for token, rel in TOKENS.items():
    if html.count(token) != 1:
        sys.exit(f"expected exactly one {token}, found {html.count(token)}")
    html = html.replace(token, base64.b64encode((HERE / rel).read_bytes()).decode())
if "_B64__" in html:
    sys.exit("unreplaced token remains")
OUT.write_text(html)
print(f"built {OUT} ({len(html) / 1024:.0f} KiB)")
