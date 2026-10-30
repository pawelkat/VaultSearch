#!/usr/bin/env python3
"""Loopback EmbeddingGemma 2 text embedder.

The Rust daemon already applies the VaultHome prefixes. This process must not
add a second prompt. Vision and audio stay unloaded. FP32 only.

    python3 -m pip install -U sentence-transformers transformers torch torchvision Pillow

torchvision and Pillow are required even for text-only use: loading the
EmbeddingGemma 2 processor imports its image processor.
    python3 contrib/embed_server.py
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import torch
from sentence_transformers import SentenceTransformer

HOST = "127.0.0.1"
PORT = 8081

model = SentenceTransformer(
    "unsloth/embeddinggemma-2",
    device="cpu",
    model_kwargs={"torch_dtype": torch.float32},
    config_kwargs={"vision_config": None, "audio_config": None},
)


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        if self.path.rstrip("/") != "/v1/embeddings":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length) or b"{}")
        texts = payload.get("input", [])
        if isinstance(texts, str):
            texts = [texts]
        if not isinstance(texts, list) or not texts:
            self.send_error(400, "input must be a non-empty string or list")
            return
        vectors = model.encode(
            texts,
            normalize_embeddings=True,
            convert_to_numpy=True,
        )
        body = {
            "data": [
                {"index": i, "embedding": row.tolist()}
                for i, row in enumerate(vectors)
            ]
        }
        raw = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, fmt: str, *args: object) -> None:
        return


if __name__ == "__main__":
    print(f"embedder listening on http://{HOST}:{PORT}/v1/embeddings", flush=True)
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
