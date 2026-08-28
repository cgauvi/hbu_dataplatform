"""Sentence-transformers embeddings for indexing and querying the corpus.

The default model is multilingual because the corpus is not: borough tables and
the resolutions they link to are written in French, and an English-only encoder
scores "école primaire" against "primary school" no better than chance.
"""

from __future__ import annotations

import contextlib
import os
from collections.abc import Iterator
from functools import cached_property

#: 1024 dimensions, ~2.2 GB. Multilingual, accepts up to 8192 tokens, and needs
#: no instruction prefix on either side. Override with URBAN_RAG_EMBEDDING_MODEL
#: - `intfloat/multilingual-e5-small` is the cheap alternative, at 384 dims.
DEFAULT_MODEL = "BAAI/bge-m3"

MODEL_ENV = "URBAN_RAG_EMBEDDING_MODEL"

#: PEM bundle for huggingface.co, when the ambient one is not the right root.
CA_BUNDLE_ENV = "URBAN_RAG_HF_CA_BUNDLE"

_INSTANCES: dict[tuple[str | None, str | None], "SentenceTransformerEmbeddings"] = {}


class SentenceTransformerEmbeddings:
    """Wraps a sentence-transformers model for indexing and querying.

    Exposes `embed_documents`/`embed_query`, the same duck-typed interface the
    rest of `rag/` (retriever, chain) relies on.

    The e5 family is trained with asymmetric `query: ` / `passage: ` prefixes and
    loses a surprising amount of accuracy without them, so they are applied
    automatically for those models and omitted for every other one - bge-m3
    included, which is trained symmetric.
    """

    def __init__(
        self,
        model_name: str | None = None,
        *,
        device: str | None = None,
        batch_size: int = 64,
        cache_dir: str | None = None,
        ca_bundle: str | None = None,
    ) -> None:
        self.model_name = model_name or os.environ.get(MODEL_ENV, DEFAULT_MODEL)
        self.device = device
        self.batch_size = batch_size
        self.cache_dir = cache_dir
        self.ca_bundle = ca_bundle
        prefixed = "e5" in self.model_name.lower()
        self._query_prefix = "query: " if prefixed else ""
        self._passage_prefix = "passage: " if prefixed else ""

    @cached_property
    def model(self):
        # Imported lazily: loading sentence-transformers costs seconds and pulls
        # torch in with it, which the `search`-free code paths never need.
        with trusted_ca(self.ca_bundle):
            from sentence_transformers import SentenceTransformer

            return SentenceTransformer(
                self.model_name, device=self.device, cache_folder=self.cache_dir
            )

    @cached_property
    def ruler(self) -> "ModelTokenRuler":
        """The model's tokenizer, for sizing chunks in the units it truncates by.

        Loaded on its own - a few MB against the model's gigabytes - so that
        chunking a corpus does not need the weights resident.
        """
        with trusted_ca(self.ca_bundle):
            from transformers import AutoTokenizer

            tokenizer = AutoTokenizer.from_pretrained(
                self.model_name, cache_dir=self.cache_dir
            )
        return ModelTokenRuler(tokenizer)

    @property
    def dimension(self) -> int:
        # Renamed in sentence-transformers 6.0; the project allows >=3.0.
        getter = getattr(self.model, "get_embedding_dimension", None)
        return int(getter() if getter else self.model.get_sentence_embedding_dimension())

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return self._encode([self._passage_prefix + text for text in texts])

    def embed_query(self, text: str) -> list[float]:
        return self._encode([self._query_prefix + text])[0]

    def _encode(self, texts: list[str], *, progress: bool = False) -> list[list[float]]:
        if not texts:
            return []
        vectors = self.model.encode(
            texts,
            batch_size=self.batch_size,
            # Normalised, so cosine distance is a plain dot product and the
            # HNSW index's 'cosine' metric matches what we compute at query
            # time without any further scaling.
            normalize_embeddings=True,
            show_progress_bar=progress,
            convert_to_numpy=True,
        )
        return vectors.tolist()

    def embed_documents_with_progress(self, texts: list[str]) -> list[list[float]]:
        """`embed_documents`, but with a progress bar - indexing takes minutes."""
        return self._encode(
            [self._passage_prefix + text for text in texts], progress=True
        )


class ModelTokenRuler:
    """Counts and cuts text the way the encoder will.

    Chunk sizes are only meaningful in the units the model truncates by, so
    chunking borrows the tokenizer instead of counting words.
    """

    def __init__(self, tokenizer) -> None:
        self._tokenizer = tokenizer

    def count(self, text: str) -> int:
        return len(self._encode(text))

    def split(self, text: str, max_tokens: int) -> list[str]:
        """Fixed-width token windows, used only on an oversized paragraph."""
        ids = self._encode(text)
        return [
            self._tokenizer.decode(ids[start : start + max_tokens]).strip()
            for start in range(0, len(ids), max_tokens)
        ]

    def _encode(self, text: str) -> list[int]:
        return self._tokenizer.encode(text, add_special_tokens=False)


def cached_embeddings(
    model_name: str | None = None,
    *,
    device: str | None = None,
    batch_size: int = 64,
    cache_dir: str | None = None,
    ca_bundle: str | None = None,
) -> SentenceTransformerEmbeddings:
    """One instance per (model, device) for the life of the process.

    Dagster runs each asset in its own step but the same process, so without
    this a three-asset job would load the weights three times.
    """
    key = (model_name, device)
    instance = _INSTANCES.get(key)
    if instance is None:
        instance = SentenceTransformerEmbeddings(
            model_name,
            device=device,
            batch_size=batch_size,
            cache_dir=cache_dir,
            ca_bundle=ca_bundle,
        )
        _INSTANCES[key] = instance
    instance.batch_size = batch_size
    return instance


@contextlib.contextmanager
def trusted_ca(ca_bundle: str | None) -> Iterator[None]:
    """Point TLS verification at `ca_bundle` (default: certifi) inside the block.

    The inverse of what the Spectrum client needs. On a laptop behind Zscaler
    the ambient `SSL_CERT_FILE` is the corporate root, which huggingface.co is
    *not* signed by - so a model download fails with `CERTIFICATE_VERIFY_FAILED`
    until these are swapped for the public bundle.

    The default is only half the story on such a machine: huggingface.co serves
    the *metadata*, then redirects the weights to a CDN - `us.aws.cdn.hf.co` -
    which *is* intercepted, and which certifi alone cannot verify. Pass a bundle
    holding both roots, or set `URBAN_RAG_HF_CA_BUNDLE` to one, and note that
    the resulting error names the wrong thing: huggingface_hub 1.x raises httpx
    exceptions, which are not `OSError` subclasses, so transformers re-raises
    them as "Can't load the model for '<name>' ... a file named
    pytorch_model.bin" with the real cause buried in the chain.
    """
    import certifi

    bundle = ca_bundle or os.environ.get(CA_BUNDLE_ENV) or certifi.where()
    names = ("SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE")
    previous = {name: os.environ.get(name) for name in names}
    os.environ.update(dict.fromkeys(names, bundle))
    try:
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
