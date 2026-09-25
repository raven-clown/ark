# ARK Console

The web console for ARK: a React app plus a small Go server that embeds
the built app and forwards `/api/v1/` to an ARK engine. The browser only
talks to this server, never to Kafka.

## Run

With the repository's compose file, `docker compose up -d --build` starts
it on [http://localhost:8088](http://localhost:8088). Sign in with an
`ARK_API_*` token; the demo uses `demo-admin-token`.

On its own:

```bash
docker build -t ark-console bridge-ui
docker run -p 8088:8088 -e ARK_ENGINE_URL=http://ark:8080 ark-console
```

| Variable | Default | Meaning |
|---|---|---|
| `ARK_ENGINE_URL` | `http://127.0.0.1:8080` | The ARK engine to talk to |
| `ARK_CONSOLE_ADDR` | `:8088` | Address to listen on |

## Develop

```bash
cd bridge-ui
npm install
ARK_ENGINE_URL=http://127.0.0.1:8080 npm run dev   # Vite on :5173, /api proxied
npm run build && go build .                        # the single binary
```

The server sends a strict content security policy (everything from its
own origin, fonts bundled), so don't add third-party scripts or styles.
