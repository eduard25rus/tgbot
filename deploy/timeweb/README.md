# Felis CRM on Timeweb Cloud

The production layout is isolated from PANTERA:

```text
/srv/felis/app       Git checkout
/srv/felis/data      contracts.db and uploads/
/srv/felis/backups   verified SQLite backups and manifests
/srv/felis/config    app.env and the project-only deploy key
/srv/felis/state     current-release and previous-release
```

Server releases run as the dedicated `felis-deploy` user (UID 10001) through
`~/.ssh/felis_timeweb_deploy_ed25519`. The PANTERA key is used only for initial
server administration. GitHub access uses a second, repository-scoped read-only
key at `/srv/felis/config/deploy_ed25519`.

The container is bound only to `127.0.0.1:8002`. Caddy terminates HTTPS. The
Compose service refuses to start when the database, the `web_users` table, or
the upload directory is absent. It never copies a production database into the
image.

## Parallel test mode

Use these values in `/srv/felis/config/app.env` while Railway remains live:

```env
BOT_ENABLED=0
BANK_MAIL_AUTO_IMPORT_ENABLED=0
DB_BACKUP_ENABLED=0
PUBLIC_BASE_URL=https://felis-test-72056842.72-56-8-42.sslip.io
```

This prevents duplicate Telegram polling, deadline notifications, mail import,
and scheduled backups. Do not enable them until the final database sync and
cutover.

Production settings can be copied into the staging config without displaying
their values:

```bash
railway run -- python3 deploy/timeweb/render_railway_env.py \
  | ssh -i ~/.ssh/pantera_timeweb_admin_ed25519 root@72.56.8.42 \
    'umask 077; tee /srv/felis/config/app.env >/dev/null'
```

Add `deploy/timeweb/Caddyfile.felis-test` as a separate block to the existing
`/etc/caddy/Caddyfile`. Never replace the file. Run `caddy fmt --overwrite`,
`caddy validate --config /etc/caddy/Caddyfile`, then `systemctl reload caddy`.

## Backups

`deploy/timeweb/backup.sh daily` uses SQLite's backup API, runs `quick_check`,
compares all table row counts, calculates SHA-256, and writes a JSON manifest.
Install a daily root cron entry only after the project files exist:

```cron
17 2 * * * /srv/felis/app/deploy/timeweb/backup.sh daily >>/var/log/felis-backup.log 2>&1
```

Every code release also creates a `predeploy` backup. Backups are kept for 30
days locally; object-storage backups can remain enabled after final cutover.

## Exact-SHA release

After tests and a commit on `main`, run:

```bash
deploy/timeweb/deploy.sh
```

The script pushes `main`, asks the server to fetch that exact 40-character SHA,
backs up SQLite, builds a SHA-tagged image, waits for `/healthz`, records the SHA
in `/srv/felis/state/current-release`, and restores the previous image if the
health check fails.

## Final cutover checklist

1. Put Railway into a maintenance/frozen-write state and stop bot/import jobs.
2. Create a final consistent SQLite backup through the SQLite backup API.
3. Download the database and uploads, verify `quick_check`, SHA-256, and table
   counts, and preserve the previous Timeweb copy in `/srv/felis/backups`.
4. Stop only `felis-crm-web`, install the final data, fix ownership, and start it.
5. Verify `/healthz`, login, major pages, row counts, and uploads.
6. Change only the `crm.felisgroup.ru` A record to `72.56.8.42`; do not change
   NS, MX, the root domain, or other projects.
7. Enable `BOT_ENABLED=1`, mail import, and backups only on Timeweb. Keep the
   stopped Railway volume and rollback backup until separately approved for
   deletion.
