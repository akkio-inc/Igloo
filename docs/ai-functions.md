# AI functions

Igloo runs Snowflake's AI functions (Cortex AI SQL) in SQL, with your own
model providers' keys: `AI_COMPLETE`, `SNOWFLAKE.CORTEX.COMPLETE`, `PROMPT`
and `AI_COUNT_TOKENS`. They call OpenAI or any server that speaks its API
(Azure OpenAI, vLLM, llama.cpp, Ollama, OpenRouter), Anthropic, Gemini,
Vertex AI, Amazon Bedrock, or a decision endpoint, through an **AI
integration** an admin creates. Igloo never resells inference: each call is
made with the integration's keys, and the provider bills you at its own
prices.

```sql
-- Admins: a provider, the models SQL may name, and who may use it.
CREATE AI INTEGRATION models PROVIDER = anthropic API_KEY = 'sk-ant-...'
  MODELS = ('claude-4-sonnet' = 'claude-sonnet-4-5-20250929', 'claude-haiku' = 'claude-haiku-4-5-20251001')
  DEFAULT_MODEL = 'claude-haiku'
  PRICES = ('claude-sonnet-4-5-20250929' = (3, 15), 'claude-haiku-4-5-20251001' = (1, 5));
GRANT USAGE ON INTEGRATION models TO ROLE analysts;

-- Analysts, in Snowflake's dialect: a one-line summary of each campaign's brief.
SELECT campaign_id,
       AI_COMPLETE(model => 'claude-4-sonnet',
                   prompt => PROMPT('Summarize this campaign brief in one line: {0}', brief)) AS summary
FROM demo.campaigns;
```

Each distinct prompt is sent once, however many rows hold it, and its
answer is kept: running the statement again, or another that asks the same
of the same model, calls no provider and costs nothing.

## AI integrations

```sql
CREATE [OR REPLACE] AI INTEGRATION [IF NOT EXISTS] name PROVIDER [=] provider [option ...]
ALTER AI INTEGRATION [IF EXISTS] name SET option ... | UNSET name, ...
DROP [AI] INTEGRATION [IF EXISTS] name
SHOW AI INTEGRATIONS
GRANT USAGE ON INTEGRATION name TO [ROLE | USER] name
REVOKE USAGE ON INTEGRATION name FROM [ROLE | USER] name
```

Only admins create, change, drop and grant integrations; a user needs
`USAGE` on one, directly or through a role, to call it. AI and
[notification integrations](alerts.md) share one set of names, as
Snowflake's integrations do. AI functions are off, and fail with SQLSTATE
`55000`, until an integration exists.

| `PROVIDER` | What it reaches | Needs |
|---|---|---|
| `openai` | OpenAI's API, `https://api.openai.com/v1`, or any server that speaks it at `BASE_URL`: vLLM, llama.cpp, Ollama (`http://host:11434/v1`), OpenRouter | `API_KEY`, unless `BASE_URL` is a server without one |
| `azure_openai` | Azure OpenAI's v1 API at `BASE_URL`, `https://<resource>.openai.azure.com/openai/v1`; models are deployments | `BASE_URL`, `API_KEY` (sent as `api-key`); `API_VERSION` adds `?api-version=` |
| `anthropic` | Anthropic's Messages API | `API_KEY` |
| `gemini` | Google's Gemini API | `API_KEY` |
| `vertex` | Gemini on Vertex AI | `SERVICE_ACCOUNT` (a service account's JSON key) with `PROJECT` and `REGION`, or an express-mode `API_KEY` |
| `bedrock` | Amazon Bedrock's Converse API | `REGION`, and `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (requests are signed, SigV4) or a Bedrock `API_KEY` |
| `decisions` | A decision endpoint that takes a state and named questions, at `BASE_URL`/decisions | `BASE_URL`, `DECISION_MODEL` |

| Option | Meaning |
|---|---|
| `BASE_URL '...'` | Where the provider's API is; by default the provider's own. HTTPS only, and only public addresses, unless `IGLOO_AI_ALLOW_HOSTS` names the host ([configuration](configuration.md#ai-functions)). |
| `API_KEY '...'`, `HEADERS ('name' = 'value', ...)`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `SERVICE_ACCOUNT '{...}'` | Credentials, stored encrypted in the metadata database under a key every replica shares, and never shown: not by `SHOW AI INTEGRATIONS`, not in errors, not in the query history. `HEADERS` are sent with every request, as a gateway's own key. |
| `API_VERSION '...'` | Anthropic's `anthropic-version` (by default `2023-06-01`), or Azure's `api-version`. |
| `REGION '...'`, `PROJECT '...'` | Bedrock's region; Vertex AI's location and project. |
| `MODELS ('name' = 'provider model', ...)` | The model names SQL may use, Snowflake's among them, and the provider's model each stands for. A name no usable integration maps fails with SQLSTATE `42704` and says how to map it; nothing falls back to another model. Map to dated snapshots (`claude-sonnet-4-5-20250929`, not `claude-sonnet-4-5`) so the same name means the same model tomorrow. |
| `DEFAULT_MODEL '...'` | The model of the functions whose model Snowflake chooses. A name in `MODELS` or the provider's own. |
| `EMBEDDING_MODEL '...'`, `DECISION_MODEL '...'` | The models of embeddings and of decisions. |
| `PRICES ('provider model' = (input, output), ...)` | US dollars per million input and output tokens, for the cost cap, `EXPLAIN` and `igloo.usage`. A model without a price is called, and counted, at no cost. |
| `MAX_CONCURRENCY n` | Requests in flight to the provider from each replica, 8 by default, at most 256. |
| `ENABLED TRUE \| FALSE` | A disabled integration is refused. |
| `COMMENT '...'` | |

When several integrations a user may use map the same model, or have a
default model, the session chooses with `SET ai_integration = 'name'`.

## Functions

| Snowflake | Igloo, as DuckDB's dialect calls it | Returns |
|---|---|---|
| `AI_COMPLETE(model, prompt [, model_parameters [, response_format [, show_details]]])` | `ai_complete(model, prompt, options)`; with `response_format` or `show_details`, `ai_complete_object` | Text; with `response_format`, an OBJECT of its schema; with `show_details => TRUE`, `{"choices": [{"messages": ...}], "created": ..., "model": ..., "usage": {"completion_tokens", "prompt_tokens", "total_tokens"}}` (`structured_output` in place of `choices` with a `response_format`) |
| `SNOWFLAKE.CORTEX.COMPLETE(model, prompt)` and `(model, [{'role': ..., 'content': ...}, ...], options)` | `ai_complete`, `ai_complete_object` | Text; with a conversation, a string holding the answer's JSON, as Snowflake's |
| `PROMPT('... {0} ... {1}', arg0, arg1, ...)` | `json_object('template', ..., 'args', json_array(...))` | An OBJECT `{"template", "args"}`, which the AI functions read as the template with the arguments in place, as text (NULL as empty) |
| `AI_COUNT_TOKENS(function, [model,] input, ...)` | `ai_count_tokens(function, model, input, options)` | The input's tokens for the model that would serve `function`: Anthropic and Gemini count them, at no cost; for other providers Igloo estimates them, about four bytes a token |

Arguments are by position or by name (`model => ...`), as in Snowflake.
`model_parameters` takes `temperature`, `top_p` and `max_tokens` (4,096 by
default); Anthropic's newer API refuses sampling settings, so Igloo never
sends them to Anthropic. `guardrails => TRUE` (Cortex Guard) is refused.
Igloo adds `refresh => TRUE`, which asks the provider again rather than
reading the cache, and keeps the new answer.

**Errors.** As in Snowflake since its 2026_02 behavior change, a row whose
own input the provider refuses or answers with something unreadable gets
NULL, and `return_error_details => TRUE` returns `{"value": ..., "error":
...}` instead; the statement then sends a NOTICE saying how many rows did.
`SNOWFLAKE.CORTEX.COMPLETE` fails the statement instead, as Snowflake's
does. A failure that is not the row's fails the statement with the
integration's name and the provider's message, its keys scrubbed: a key the
provider refuses (401, 403), a model or endpoint it does not have (404), a
spend limit it enforces, or a provider that does not answer: requests
refused for their rate (429, with `retry-after` honored) or failing on the
provider's side (5xx, 529) or timing out are sent again, five times in all
over about 8 seconds plus what `retry-after` asks, before they fail it.

## Caps

A statement makes at most `IGLOO_AI_MAX_CALLS` calls to providers (1,000
by default) whose estimated cost at the integrations' prices stays under
`IGLOO_AI_MAX_COST_USD` ($1 by default). One that would go over fails
before it sends the call (SQLSTATE `54000`), naming the cap; distinct
inputs the cache answers are not calls. An admin raises a role's caps, or
lifts them with 0, as with `statement_timeout`; a session can lower its
own:

```sql
ALTER ROLE analysts SET ai_max_calls = 50000;
ALTER ROLE analysts SET ai_max_cost_usd = 25;
SET ai_max_calls = 100;            -- this session's statements, never above the role's
SHOW ai_max_calls;
```

Where several of a user's roles set a cap, the most generous applies.

## Cost and usage

`EXPLAIN` of a query that calls AI functions says what they would send,
without sending it: Igloo runs the query with each call counted rather
than made, and adds rows such as

```
ai calls | models/claude-sonnet-4-5-20250929: 1240 distinct inputs to send, about 186000 input and at most 1269760 output tokens, about $19.6044
ai cache | 310 distinct inputs answered from the cache, at no cost
ai caps  | at most 1000 calls and $1 a statement (0 is no cap)
```

so that a statement over its caps can be found before it runs. Output
tokens are bounded by `max_tokens`, which is why their estimate is high.

The query history records each statement's calls, cache hits, tokens and
estimated cost (`ai_calls`, `ai_cache_hits`, `ai_input_tokens`,
`ai_output_tokens`, `ai_cost_usd`, `ai_models` in `igloo.query_history`),
and `igloo.usage` sums them by day, user, role and query tag, for
chargeback to clients and campaigns.

## The cache

Answers are kept in the metadata database, for every replica, for
`IGLOO_AI_CACHE_RETENTION` (30 days by default), keyed by a SHA-256 of the
function, the provider and its address, the provider's model, the version
of Igloo's template for the function, the options and the input. A rerun
returns the same answer and costs nothing, which providers' sampling
settings cannot promise: repeatability comes from the cache. `refresh =>
TRUE` asks again, and `IGLOO_AI_CACHE=off` keeps nothing.

Each answer is tagged with the tables its statement read: those DuckDB
loaded from the catalog, views read through, and every table the
statement's parse names. A statement that may read files, or that Igloo
could not analyze, tags its answers as derived from any table.
[Erasure requests](privacy.md) purge every answer derived from a table they
erase from, the person's or not, and keep statements that began before
them from keeping anything more; a request does not complete while any
such answer remains.

## Where they run

The functions run in Igloo's own process, as DuckDB scalar functions, and
their calls go out through Igloo's egress guard: never through a DuckDB
extension, whose HTTP client Igloo's lockdown of DuckDB would not see.
Calls go out from the coordinator, which holds the integrations' keys:
with [distributed execution](distributed-execution.md), workers read,
filter and aggregate, and the coordinator calls the provider for what they
return; workers hold no provider's keys. Calls of one statement on DuckDB's
threads share one request per distinct input, and each integration has at
most `MAX_CONCURRENCY` requests in flight per replica.

They run in queries, `INSERT ... SELECT`, `CREATE TABLE ... AS` and
tasks' statements, in either dialect. In a cursor, `COPY ... TO`, or over
Arrow Flight SQL they fail (SQLSTATE `0A000`).

## Governance

- **Policies apply first.** Masking and row access policies rewrite a
  statement before it runs, so a function sees, and a provider receives,
  only what the user may see: a masked email reaches the provider masked.
- **Clean rooms.** A statement that calls AI functions and reads a table a
  [clean room](clean-rooms.md) confines is refused, admins included: a
  dataset's rows are for the room's approved templates alone.
- **Records.** The history records each statement's models, tokens and
  cost; the log records each change of an integration and who made it.

## Known differences

- Images, documents and audio (`TO_FILE`, the file forms of the functions)
  are not supported, nor are fine-tuned models or Cortex Guard.
- `AI_COUNT_TOKENS` estimates tokens for providers that cannot count them.
- An Iceberg table's `VECTOR` column is a list (see [Snowflake
  SQL](snowflake-sql.md#types)).
