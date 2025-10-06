#!/bin/zsh
set -euo pipefail
git rev-parse --is-inside-work-tree >/dev/null

RAW="$(pbpaste)"

# Вырезаем тело heredoc из "Copy as git apply"
if [[ "$RAW" == *"<<"'EOF'*"EOF"* ]]; then
  BODY=$(print -r -- "$RAW" | awk '
    /<<'\''EOF'\''/ {inb=1; next}
    inb && /^EOF$/ {inb=0; exit}
    inb {print}
  ')
else
  BODY="$RAW"
fi

# Базовая валидация
if ! print -r -- "$BODY" | grep -q $'\n'; then
  echo "[ERR] Clipboard looks like a single line (no newlines). Save patch to a file or recopy as plain text."
  printf "%s" "$BODY" > PATCH.clipboard.txt
  exit 1
fi

# Снимаем ```...```
BODY="$(print -r -- "$BODY" | sed '1s/^```[[:alnum:]_-]*[[:space:]]*//; $s/[[:space:]]*```$//')"

# От первого diff/From
if print -r -- "$BODY" | grep -qE '^[[:space:]]*From [0-9a-f]{40} '; then
  BODY="$(print -r -- "$BODY" | awk 'f||/^[[:space:]]*From [0-9a-f]{40} /{f=1;print}')"
elif print -r -- "$BODY" | grep -qE '^[[:space:]]*diff --git '; then
  BODY="$(print -r -- "$BODY" | awk 'f||/^[[:space:]]*diff --git /{f=1;print}')"
fi

TMP="$(mktemp -t codex_patch.XXXXXX.patch)"
print -r -- "$BODY" > "$TMP"
echo "[i] Patch saved to $TMP"

# Проверки структуры
grep -qE '^[[:space:]]*diff --git ' "$TMP" || { echo "[ERR] No 'diff --git' headers found."; exit 1; }
grep -qE '^[[:space:]]*@@ '        "$TMP" || { echo "[ERR] No hunks ('@@ ... @@') found."; head -n 30 "$TMP"; exit 1; }

# Применение
if grep -qE '^[[:space:]]*From [0-9a-f]{40} ' "$TMP" && grep -qE '^[[:space:]]*Subject:' "$TMP"; then
  echo "[i] Trying: git am -3"
  git am -3 < "$TMP" || { git am --abort || true; echo "[i] git am failed. Trying git apply"; git apply --check --whitespace=fix "$TMP"; git apply --whitespace=fix "$TMP"; git add -A; git commit -m "Apply patch from Codex"; }
else
  echo "[i] Trying: git apply --check/--whitespace=fix"
  git apply --check --whitespace=fix "$TMP"
  git apply --whitespace=fix "$TMP"
  git add -A
  git commit -m "Apply patch from Codex"
fi

git pull --rebase --autostash origin main || true
git push origin HEAD:main
echo "[DONE] Synced with GitHub."
