"""The LangChain question-answering chain over the retrieved chunks.

Generation runs a local open-weights instruct model through
`transformers`, so answering needs no API key and no egress once the weights are
cached. The trade is speed: on CPU a short answer takes tens of seconds.
"""

from __future__ import annotations

import os
from collections.abc import Sequence

from langchain_core.documents import Document
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import Runnable, RunnableParallel, RunnablePassthrough

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


def format_documents(documents: Sequence[Document]) -> str:
    """Number the passages so the model has something concrete to cite.

    The source filename rather than the whole URL: it is what identifies a
    resolution to a reader ("PP_CA11140080"), and it keeps a hundred characters
    of city URL out of every extract's header.
    """
    return "\n\n".join(
        f"[{position}] {_source_label(document)}\n{document.page_content}"
        for position, document in enumerate(documents, start=1)
    )


def _source_label(document: Document) -> str:
    url = str(document.metadata.get("url", ""))
    name = url.rsplit("/", 1)[-1] or document.metadata.get("doc_id", "?")
    return f"{name} (extrait {document.metadata.get('chunk_index', 0) + 1})"


def build_llm(
    model_id: str | None = None,
    *,
    max_new_tokens: int = 256,
    device: str | None = None,
    ca_bundle: str | None = None,
) -> Runnable:
    """A local HuggingFace instruct model, wrapped as a LangChain chat model."""
    # Imported here rather than at module scope: loading transformers and torch
    # costs several seconds, and `urban-rag search` never generates anything.
    import torch
    from langchain_huggingface import ChatHuggingFace, HuggingFacePipeline
    from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline

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

    generator = pipeline(
        "text-generation",
        model=model,
        tokenizer=tokenizer,
        max_new_tokens=max_new_tokens,
        # Greedy: two identical questions should not get different answers, and
        # grounded extraction is not a task that benefits from sampling.
        do_sample=False,
        # Without this the prompt is echoed back as part of the completion.
        return_full_text=False,
    )
    return ChatHuggingFace(
        llm=HuggingFacePipeline(pipeline=generator), tokenizer=tokenizer
    )


def build_chain(retriever: Runnable, llm: Runnable) -> Runnable:
    """Question in, `{question, documents, context, answer}` out.

    The retrieved documents are kept on the output so the caller can show which
    rows an answer came from, rather than trusting the citations in the prose.
    """
    prompt = ChatPromptTemplate.from_messages([("system", _SYSTEM), ("human", _HUMAN)])
    return (
        RunnableParallel(question=RunnablePassthrough(), documents=retriever)
        | RunnablePassthrough.assign(
            context=lambda state: format_documents(state["documents"])
        )
        | RunnablePassthrough.assign(answer=prompt | llm | StrOutputParser())
    )
