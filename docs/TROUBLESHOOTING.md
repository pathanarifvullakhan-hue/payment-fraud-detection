# Troubleshooting

| Symptom | Fix |
|---|---|
| Red "unknown namespace/element" in XML | a module is missing: Mule Palette -> Add Modules (HTTP, APIkit, Object Store, Database, Email, VM, Secure Properties, API Gateway) |
| `Could not resolve placeholder 'secure.key'` or decrypt error | VM argument `-M-Dsecure.key=mySecretKey` missing, or the value was encrypted with a different key/algorithm (use Blowfish + CBC both times) |
| `No suitable driver` / `Driver class not found` | mysql-connector-j dependency or `sharedLibraries` missing; Maven -> Update Project |
| `Access denied for user 'fraud_app'` | encrypted DB password does not match `ChangeMe!123`, or schema.sql was not run |
| `Table ... doesn't exist` | schema.sql not run in database `frauddb` |
| App fails: autodiscovery / `api.id` | rename `autodiscovery.xml` to `.off` for local runs |
| `Unknown error type ...` on start | an error `type="..."` is not valid in your connector version; delete that name from the list, keep the rest (`ANY` always works) |
| `vm:listener ... transactionalAction` attribute rejected | delete that attribute from the listener (and from `dead-letter.xml`); if the DB step then complains about a transaction, set the DB `try` in `persistence.xml` to `transactionalAction="ALWAYS_BEGIN"` (already so) |
| Port 8081 in use | change `http.port` |
| 500 on every call | read the log line `UNEXPECTED_ERROR type=...` |
| Email not arriving | MailHog running? `smtp.host=localhost`, `smtp.port=1025`; if the SMTP server rejects auth, remove `user`/`password` from `<email:smtp-connection>` in `global-config.xml` |
| No alert for HIGH event | check `alert_status`; `PENDING` older than 1 min = SMTP issue, reconciler retries |
| Messages from before restart reappear | expected: queues are persistent. To wipe, stop the app and delete the project's `.mule` folder |

## Mac-specific
| Symptom | Fix |
|---|---|
| "Anypoint Studio can't be opened because it is from an unidentified developer" | Finder -> Applications -> right-click Studio -> Open -> Open. Or System Settings -> Privacy & Security -> "Open Anyway" |
| `java: command not found` or Studio says wrong Java | `brew install --cask temurin@17`, then `export JAVA_HOME=$(/usr/libexec/java_home -v 17)` (put the same line in `~/.zshrc`) and restart Studio |
| `docker: command not found` / "Cannot connect to the Docker daemon" | Open the Docker Desktop app and wait until it says "Running" |
| MailHog warning "platform (linux/amd64) does not match (linux/arm64)" | Normal on M1/M2/M3/M4 Macs. It still works (runs emulated). The setup script already passes `--platform linux/amd64` |
| `port is already allocated` for 3306 | A local MySQL is running. Stop it (`brew services stop mysql`) or change `-p 3306:3306` to `-p 3307:3306` and set `db.port: "3307"` in `config-dev.yaml` |
| `Permission denied` running a script | `chmod +x scripts/*.sh` |
| Can't see `.gitignore` in Finder | Press Cmd + Shift + . (dot) to show hidden files |
