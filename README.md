# mrblean-service

Mock checkout service used as **evidence code** for the MrBlean AI SRE lab.

This repository is the canonical source for `lab/mrblean_service` in
[Tradunsky/ai-sre](https://github.com/Tradunsky/ai-sre). Lab compose builds
from this tree; E2E scenarios that claim a deploy RCA **commit broken changes
here** (and push to GitHub) so the coding agent can check out the deploy SHA
and find a real root cause.

## Layout

- `app.py` — FastAPI service with Prometheus metrics, Loki logs, and `/lab/fault`
- `Dockerfile` — image built by `lab/docker-compose.yml`
- `requirements.txt`

## Healthy vs broken deploys

Healthy baseline keeps:

```python
CHECKOUT_PAYMENT_VALIDATION_BROKEN = False
```

The `deploy_500s` E2E scenario commits a flip to `True`, rebuilds the container
with `DEPLOY_SHA` / `DEPLOY_VERSION` set to that commit, and generates checkout
500s + logs that cite the SHA.

## Run locally (via ai-sre lab)

```bash
cd lab
DOCKER_HOST=unix:///run/user/1000/docker.sock docker compose up -d --build mrblean-service
```
