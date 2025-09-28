#!/bin/zsh
set -e

# 1) Пробуем применить как git-format (git am)
if pbpaste | git am -3 2>/dev/null; then
  echo "[OK] Applied via git am"
else
  git am --abort >/dev/null 2>&1 || true
  echo "[i] git am failed. Trying git apply…"

  if pbpaste | git apply --check --whitespace=fix 2>/dev/null; then
    pbpaste | git apply --whitespace=fix
    git add -A
    echo "[OK] Applied via git apply + staged"
  else
    echo "[i] git apply --check failed. Fallback to --reject…"
    TMP=$(mktemp /tmp/codex_patch.XXXXXX.patch)
    pbpaste > "$TMP"
    if git apply --reject --whitespace=fix "$TMP"; then
      git add -A
      echo "[OK] Applied via git apply --reject (check .rej if needed)"
    else
      echo "[ERR] Failed to apply patch. Verify patch format."
      exit 1
    fi
  fi
fi

# 2) Коммитим, если изменений нет — пропускаем
if git diff --cached --quiet && git diff --quiet; then
  echo "[i] Nothing to commit (git am likely created a commit)."
else
  git commit -m "Apply patch from Codex" || true
fi

# 3) Синхронизация
git pull --rebase --autostash || true
git push
echo "[DONE] Synced with GitHub."
