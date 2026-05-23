# Zephost beta backend

Minimal 4-user beta queue for the Zephost distributed compute dashboard.

## API

Base URL for the hosted beta:

```text
https://sever-for-ai-ladsharing-1.onrender.com
```

Create a task after login. The backend now owns `user_id` from the bearer token or API key; clients should not send a trusted user id:

```js
await fetch("https://sever-for-ai-ladsharing-1.onrender.com/task", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    file_name: "prompt.json",
    quality: "alpha",
    payload: {
      job_type: "ai_inference",
      model: "llama3.2",
      prompt: "Write a one-line launch checklist for Zephost."
    }
  })
});
```

Worker fetches the next task:

```js
const next = await fetch("https://sever-for-ai-ladsharing-1.onrender.com/task")
  .then((res) => res.json());
```

Worker submits a result:

```js
await fetch("https://sever-for-ai-ladsharing-1.onrender.com/result", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    task_id: 1,
    result: { message: "processed render task", simulated_seconds: 3 }
  })
});
```

Read queue stats:

```js
const status = await fetch("https://sever-for-ai-ladsharing-1.onrender.com/status")
  .then((res) => res.json());
```

## Local commands

```bash
cargo run --bin zephost
cargo run --bin worker
```

## Environment

```text
DATABASE_URL=postgres://...
ALLOW_PUBLIC_SIGNUP=false
ZEPHOST_ADMIN_USERNAME=admin
ZEPHOST_ADMIN_PASSWORD=change-me
ZEPHOST_API_BASE_URL=http://127.0.0.1:8080
OLLAMA_URL=http://127.0.0.1:11434
```

When `DATABASE_URL` is present, Zephost creates the PostgreSQL tables it needs for users, API keys, tasks, job logs, and waitlist entries. Public sign-up is closed unless `ALLOW_PUBLIC_SIGNUP=true`; set `ZEPHOST_ADMIN_USERNAME` and `ZEPHOST_ADMIN_PASSWORD` to seed the first invited operator.

The worker polls `/task`, runs `ai_inference` jobs through an Ollama-compatible local API, then submits success or failure plus a reason to `/result`.

## Admin

Set `ZEPHOST_ADMIN_USERNAME` and `ZEPHOST_ADMIN_PASSWORD` to seed the first operator account, then use that account to access:

```text
GET /admin/waitlist
```

Render should run the backend with:

```bash
cargo run --bin zephost
```
