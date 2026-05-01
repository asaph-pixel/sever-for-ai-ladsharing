# Zephost beta backend

Minimal 4-user beta queue for the Zephost distributed compute dashboard.

## API

Base URL for the hosted beta:

```text
https://sever-for-ai-ladsharing-1.onrender.com
```

Create a task:

```js
await fetch("https://sever-for-ai-ladsharing-1.onrender.com/task", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    user_id: "beta-user-1",
    task_type: "render",
    payload: { input: "sample-file.mp4", quality: "beta" }
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
cargo run --features worker --bin worker
```

Render should run the backend with:

```bash
cargo run --bin zephost
```
