# Production deployment rules

- Production runs on Timeweb Cloud at `72.56.8.42` under `/srv/felis`.
- Railway is the legacy host and must not be treated as production after the final cutover is confirmed.
- Every completed code change must pass tests, be committed, pushed to `origin/main`, and deployed to Timeweb by exact Git SHA.
- An ordinary code deploy must never replace `/srv/felis/data/contracts.db` or user uploads.
- Every release requires a server-side SQLite backup, health check, and automatic image rollback on failure.
- Do not commit unrelated changes from a dirty worktree.
- Never commit `.env` files, databases, SSH keys, VAPID private keys, passwords, or backup archives.
- Do not modify `/srv/pantera`, the PANTERA container, or its Caddy site block.
