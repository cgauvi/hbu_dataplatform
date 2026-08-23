"""The question-answering chain over the retrieved chunks.

Generation runs a local open-weights instruct model through
`transformers`, so answering needs no API key and no egress once the weights are
cached. The trade is speed: on CPU a short answer takes tens of seconds.
"""

from __future__ import annotations

import os
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from urban_rag.rag.store import Hit

#: Small enough to run on a laptop CPU, multilingual enough to answer about a
#: French corpus, and Apache-2.0. Override with URBAN_RAG_LLM_MODEL - the same
#: code path takes any instruct model with a chat template.
DEFAULT_LLM_MODEL = "Qwen/Qwen2.5-1.5B-Instruct"

LLM_MODEL_ENV = "URBAN_RAG_LLM_MODEL"

_SYSTEM = """\
Tu es un assistant qui répond à des questions sur les données d'urbanisme de \
la Ville de Montréal (zonage, écoles, apaisement de la circulation, patrimoine).

Règles:
- Réponds UNIQUEMENT à partir des extraits fournis. N'invente aucune donnée.
- Si les extraits ne contiennent pas la réponse, dis-le clairement.
- Cite les extraits utilisés par leur numéro, par exemple [1].
- Réponds dans la langue de la question, de façon brève et factuelle."""

_HUMAN = """\
Extraits:
{context}

Question: {question}"""


class ChatModel(Protocol):
    """Whatever turns a chat message list into a reply."""

    def invoke(self, messages: list[dict[str, str]]) -> str: ...


class Retriever(Protocol):
    """Whatever turns a question into the passages that might answer it."""

    def get_relevant_documents(self, query: str) -> list[Hit]: ...


def format_documents(documents: Sequence[Hit]) -> str:
    """Number the passages so the model has something concrete to cite.

    The source filename rather than the whole URL: it is what identifies a
    resolution to a reader ("PP_CA11140080"), and it keeps a hundred characters
    of city URL out of every extract's header.
    """
    return "\n\n".join(
        f"[{position}] {_source_label(document)}\n{document.text}"
        for position, document in enumerate(documents, start=1)
    )


def _source_label(document: Hit) -> str:
    url = str(document.metadata.get("url", ""))
    name = url.rsplit("/", 1)[-1] or document.metadata.get("doc_id", "?")
    return f"{name} (extrait {document.metadata.get('chunk_index', 0) + 1})"


class LocalChatModel:
    """A local HuggingFace instruct model, driven through its chat template."""

    def __init__(self, model, tokenizer, *, device: str, max_new_tokens: int) -> None:
        self.model = model
        self.tokenizer = tokenizer
        self.device = device
        self.max_new_tokens = max_new_tokens

    def invoke(self, messages: list[dict[str, str]]) -> str:
        import torch

        prompt = self.tokenizer.apply_chat_template(
            messages, tokenize=False, add_generation_prompt=True
        )
        inputs = self.tokenizer(prompt, return_tensors="pt").to(self.device)
        with torch.no_grad():
            generated_ids = self.model.generate(
                **inputs,
                max_new_tokens=self.max_new_tokens,
                # Greedy: two identical questions should not get different
                # answers, and grounded extraction is not a task that benefits
                # from sampling.
                do_sample=False,
            )
        # Only the completion, not the echoed prompt.
        completion_ids = generated_ids[0][inputs["input_ids"].shape[-1] :]
        return self.tokenizer.decode(completion_ids, skip_special_tokens=True).strip()


def build_llm(
    model_id: str | None = None,
    *,
    max_new_tokens: int = 256,
    device: str | None = None,
    ca_bundle: str | None = None,
) -> LocalChatModel:
    """A local HuggingFace instruct model, wrapped as a chat model."""
    # Imported here rather than at module scope: loading transformers and torch
    # costs several seconds, and `urban-rag search` never generates anything.
    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer

    from urban_rag.rag.embeddings import trusted_ca

    model_id = model_id or os.environ.get(LLM_MODEL_ENV, DEFAULT_LLM_MODEL)
    if device is None:
        device = "cuda" if torch.cuda.is_available() else "cpu"

    # Same swap the encoder download needs: huggingface.co is not behind the
    # inspecting proxy, so verifying it against the corporate root fails.
    with trusted_ca(ca_bundle):
        tokenizer = AutoTokenizer.from_pretrained(model_id)
        model = AutoModelForCausalLM.from_pretrained(
            model_id,
            # bfloat16 rather than float32 on CPU too: it halves the resident
            # size (a 1.5B model is 6 GB in float32, 3 GB here), and `ask` holds
            # the encoder in the same process, so the pair is what has to fit.
            dtype=torch.float16 if device == "cuda" else torch.bfloat16,
            low_cpu_mem_usage=True,
        ).to(device)

    return LocalChatModel(model, tokenizer, device=device, max_new_tokens=max_new_tokens)


@dataclass
class RagChain:
    """Question in, `{question, documents, context, answer}` out."""

    retriever: Retriever
    llm: ChatModel

    def invoke(self, question: str) -> dict[str, object]:
        """Answer `question`, keeping the retrieved documents on the result.

        They ride along so the caller can show which rows an answer came from,
        rather than trusting the citations in the prose.
        """
        documents = self.retriever.get_relevant_documents(question)
        context = format_documents(documents)
        messages = [
            {"role": "system", "content": _SYSTEM},
            {"role": "user", "content": _HUMAN.format(context=context, question=question)},
        ]
        answer = self.llm.invoke(messages)
        return {
            "question": question,
            "documents": documents,
            "context": context,
            "answer": answer,
        }


def build_chain(retriever: Retriever, llm: ChatModel) -> RagChain:
    return RagChain(retriever=retriever, llm=llm)

