# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Lab 05 · Build, trace and evaluate a support agent (code-first)
# MAGIC Databricks Free Edition does not include the Agent Bricks *Knowledge Assistant*, so we build the same pattern
# MAGIC in ~60 lines: an LLM from **Foundation Model APIs** decides which **governed tools** to call —
# MAGIC a policy search over `support_docs` and the Unity Catalog functions `lookup_order` / `customer_order_summary`.
# MAGIC **MLflow 3** traces every step and evaluates quality with LLM judges.
# MAGIC
# MAGIC Prerequisite: run `05_agent_tools` first.

# COMMAND ----------

# MAGIC %pip install -q --upgrade "mlflow[databricks]>=3.1" openai databricks-sdk
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct",
                     "Pay-per-token chat model (see Serving page)")
llm_endpoint = dbutils.widgets.get("llm_endpoint")

import json
import re

import mlflow
from databricks.sdk import WorkspaceClient

mlflow.set_experiment(f"/Users/{_user}/bootcamp-support-agent")
mlflow.openai.autolog()                       # LLM calls become spans automatically
llm = WorkspaceClient().serving_endpoints.get_open_ai_client()

# COMMAND ----------

# MAGIC %md ## 1 · Tools — each one is governed data or a governed function

# COMMAND ----------

TOOLS = [
    {"type": "function", "function": {
        "name": "search_policies",
        "description": "Search the customer-support policy documents (returns, shipping, loyalty, passwords).",
        "parameters": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}}},
    {"type": "function", "function": {
        "name": "lookup_order",
        "description": "Look up one order by its order number: date, amount, channel, category, region.",
        "parameters": {"type": "object", "properties": {"order_id": {"type": "integer"}}, "required": ["order_id"]}}},
    {"type": "function", "function": {
        "name": "customer_order_summary",
        "description": "Summarise a customer's order history: number of orders, total spend, last order date.",
        "parameters": {"type": "object", "properties": {"customer_id": {"type": "integer"}},
                       "required": ["customer_id"]}}},
]

DOCS = {r.doc_id: r.content for r in spark.table("support_docs").collect()}


@mlflow.trace(span_type="RETRIEVER")
def search_policies(query: str, k: int = 2) -> str:
    """Tiny keyword retriever over support_docs (swap for AI Search in the stretch)."""
    words = set(re.findall(r"[a-z0-9]+", query.lower()))
    ranked = sorted(DOCS.items(), key=lambda kv: -len(words & set(re.findall(r"[a-z0-9]+", kv[1].lower()))))
    return "\n\n".join(f"[{doc_id}]\n{text}" for doc_id, text in ranked[:k])


@mlflow.trace(span_type="TOOL")
def call_uc_function(name: str, arg: int) -> str:
    rows = spark.sql(f"SELECT * FROM {name}({int(arg)})").toPandas().to_dict("records")
    return json.dumps(rows, default=str) if rows else "No matching record."


def run_tool(name: str, args: dict) -> str:
    if name == "search_policies":
        return search_policies(args["query"])
    if name == "lookup_order":
        return call_uc_function("lookup_order", args["order_id"])
    if name == "customer_order_summary":
        return call_uc_function("customer_order_summary", args["customer_id"])
    return f"Unknown tool {name}"

# COMMAND ----------

# MAGIC %md ## 2 · The agent loop

# COMMAND ----------

SYSTEM_PROMPT = (
    "You are a polite customer-support assistant. Use the tools to answer. "
    "Answer only from tool results and cite the policy document name in brackets. "
    "If the tools do not contain the answer, say you don't know. "
    "Never promise a refund: ask the customer to confirm first."
)


@mlflow.trace(span_type="AGENT")
def support_agent(question: str) -> str:
    messages = [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": question}]
    # SOLUTION-BEGIN lab-05: Loop up to 5 times: call llm.chat.completions.create(model=llm_endpoint, messages=messages, tools=TOOLS); if the reply has no tool_calls return its content; otherwise append the assistant message, run each tool with run_tool() and append {"role": "tool", "tool_call_id": ..., "content": result}.
    for _ in range(5):
        response = llm.chat.completions.create(model=llm_endpoint, messages=messages, tools=TOOLS)
        message = response.choices[0].message
        if not message.tool_calls:
            return message.content
        messages.append(message.model_dump(exclude_none=True))
        for call in message.tool_calls:
            result = run_tool(call.function.name, json.loads(call.function.arguments or "{}"))
            messages.append({"role": "tool", "tool_call_id": call.id, "content": result})
    # SOLUTION-END
    return "Sorry, I could not complete this request."

# COMMAND ----------

print(support_agent("What did order 1 contain, and can I still return it?"))
# Open the trace below the cell: you see the LLM calls, the lookup_order tool and the policy search.

# COMMAND ----------

# MAGIC %md ## 3 · Evaluate with MLflow 3 LLM judges

# COMMAND ----------

from mlflow.genai.scorers import Correctness, Guidelines, RelevanceToQuery, Safety

eval_data = [
    {"inputs": {"question": "How many days do I have to return a laptop?"},
     "expectations": {"expected_response": "Products can be returned within 30 days of delivery for a full refund."}},
    {"inputs": {"question": "Can I combine WELCOME10 with another coupon?"},
     "expectations": {"expected_response": "No, WELCOME10 cannot be combined with other offers."}},
    {"inputs": {"question": "How long is a password reset link valid?"},
     "expectations": {"expected_response": "The reset link is valid for 30 minutes."}},
    {"inputs": {"question": "How long does standard shipping take to APAC?"},
     "expectations": {"expected_response": "Standard shipping to APAC takes 5-8 business days."}},
    {"inputs": {"question": "Can I get a refund on an activated software licence?"},
     "expectations": {"expected_response": "No, software licences are refundable only if the key was not activated."}},
]

# SOLUTION-BEGIN lab-05: Call mlflow.genai.evaluate with eval_data, predict_fn=support_agent and scorers Correctness, RelevanceToQuery, Safety and a Guidelines judge requiring a polite answer that does not invent policies.
results = mlflow.genai.evaluate(
    data=eval_data,
    predict_fn=support_agent,
    scorers=[
        Correctness(),
        RelevanceToQuery(),
        Safety(),
        Guidelines(name="policy_grounded",
                   guidelines="The answer must be polite and must not invent policies that are not in the support documents."),
    ],
)
# SOLUTION-END
print(results.metrics)
# Experiments → bootcamp-support-agent → Evaluations: per-question scores, rationales and traces.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Human in the loop
# MAGIC Ask *"Refund order 1 now."* — the agent must ask for confirmation (system prompt rule). Remove the rule,
# MAGIC re-run the evaluation with a Guidelines judge *"never promises refunds without confirmation"* and compare.
# MAGIC
# MAGIC ## Stretch
# MAGIC * **AI Search:** Catalog → `support_docs` → **Create** → **Vector search index** (Delta Sync, managed embeddings,
# MAGIC   on `content`) and replace `search_policies` with `VectorSearchClient().get_index(...).similarity_search(...)`.
# MAGIC   Free Edition allows one AI Search endpoint.
# MAGIC * **Genie as a tool:** add a tool that asks your lab 03 Genie Agent revenue questions.

# COMMAND ----------

print(support_agent("Refund order 1 now."))