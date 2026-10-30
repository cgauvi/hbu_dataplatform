"""A local instruct model that answers in JSON of a given schema.

`rag.chain.LocalChatModel` runs a small model through `transformers` for
`urban-rag ask`. Structured extraction wants a larger one and a hard
guarantee on the shape of the answer, and that is llama.cpp's territory: a
14B model as a 4-bit GGUF fits a 32 GB laptop beside the encoder where the
bf16 weights (~30 GB) would not, and its grammar-constrained decoding
compiles a JSON schema into a GBNF grammar so the model *cannot* answer
anything the schema refuses. `LlamaJsonModel` is that, behind the same
`invoke(messages) -> str` the chain's `ChatModel` Protocol names, so a test
swaps in a fake and the asset never knows.

Qwen3 thinks by default. Extraction wants no thinking - the grammar forces
`{` as the first token anyway - so the prompt is rendered as ChatML by hand
with the empty think block Qwen's own template emits for
`enable_thinking=False`, and `/no_think` is left to the caller's user turn.

`llama_cpp` is imported inside the functions that need it, like
`sentence_transformers` in `rag.embeddings`: the slim image loads this
module, and only the `llm` extra installs the wheel.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

#: The assistant turn Qwen3's chat template opens with when thinking is off.
_ASSISTANT_PREFIX = "<|im_start|>assistant\n<think>\n\n</think>\n\n"

#: One model per process, like `cached_embeddings`: the weights are 8 GB and
#: a Dagster run materializes several assets in one process.
_MODELS: dict[str, Any] = {}


def render_chatml(messages: list[dict[str, str]]) -> str:
    """Qwen's ChatML, rendered without the tokenizer: one turn per message,
    then the open assistant turn with its empty think block."""
    parts = [f"<|im_start|>{m['role']}\n{m['content']}<|im_end|>\n" for m in messages]
    return "".join(parts) + _ASSISTANT_PREFIX


def grammar_for(schema: Mapping[str, Any]):
    """The GBNF grammar llama.cpp compiles from a JSON schema."""
    from llama_cpp import LlamaGrammar

    return LlamaGrammar.from_json_schema(json.dumps(dict(schema)))


def load_llama(
    *,
    repo_id: str,
    filename: str,
    model_path: str | None = None,
    n_ctx: int = 4096,
    n_threads: int | None = None,
    ca_bundle: str | None = None,
    verbose: bool = False,
):
    """The llama.cpp model: a local GGUF when `model_path` names one, else
    the file pulled from the hub through `huggingface_hub` - behind the
    corporate proxy that needs `trusted_ca`, like the encoder's download."""
    from llama_cpp import Llama

    from hbu_dataplatform.rag.embeddings import trusted_ca

    threads = n_threads or max(1, (os.cpu_count() or 2) - 2)
    common = {"n_ctx": n_ctx, "n_threads": threads, "n_batch": 512, "use_mmap": True, "verbose": verbose}
    if model_path:
        return Llama(model_path=str(Path(model_path)), **common)
    with trusted_ca(ca_bundle):
        return Llama.from_pretrained(repo_id=repo_id, filename=filename, **common)


class LlamaJsonModel:
    """A llama.cpp model bound to one grammar: `invoke` returns the JSON text."""

    def __init__(self, llama, *, grammar, max_tokens: int = 400, model_id: str = "") -> None:
        self.llama = llama
        self.grammar = grammar
        self.max_tokens = max_tokens
        self.model_id = model_id

    def invoke(self, messages: list[dict[str, str]]) -> str:
        completion = self.llama(
            render_chatml(messages),
            max_tokens=self.max_tokens,
            # Greedy: the same text should read the same way twice.
            temperature=0.0,
            grammar=self.grammar,
            stop=["<|im_end|>"],
        )
        return completion["choices"][0]["text"].strip()

    def count_tokens(self, text: str) -> int:
        return len(self.llama.tokenize(text.encode("utf-8"), add_bos=False))


def cached_json_model(key: str, factory: Callable[[], LlamaJsonModel]) -> LlamaJsonModel:
    """The model built by `factory`, once per process per `key`."""
    model = _MODELS.get(key)
    if model is None:
        model = factory()
        _MODELS[key] = model
    return model


__all__ = ["LlamaJsonModel", "cached_json_model", "grammar_for", "load_llama", "render_chatml"]
