#!/usr/bin/env zsh
set -euo pipefail

git rev-parse --is-inside-work-tree >/dev/null

# temp файлы (BSD mktemp)
TMPBASE="$(mktemp -t codex_patch)"
RAW="${TMPBASE}.raw"
FIX="${TMPBASE}.patch"

# буфер → файл + нормализация перевода строк
pbpaste | perl -pe 's/\r\n/\n/g; s/\r/\n/g' > "$RAW"

# Валидация: это вообще diff?
if ! grep -qE '^diff --git ' "$RAW"; then
  echo "[ERR] В буфере нет заголовков 'diff --git'. Попросите Codex выдать unified diff или 'Copy as git apply'."
  rm -f "$RAW" 2>/dev/null || true
  exit 1
fi

# Нормализация «стрелочного» формата diff --git .* => path  (best-effort)
if grep -qE '^diff --git .*=> ' "$RAW"; then
  echo "[i] Обнаружен «стрелочный» формат. Пробую нормализовать…"
  awk '
    /^diff --git .*=> .*/ { match($0, /=>[[:space:]]+(.+)$/, m); dst=m[1]; print "diff --git a/" dst " b/" dst; skip=1; next }
    { print }
  ' "$RAW" > "$FIX.tmp1"

  awk '
    /^diff --git a\/.+ b\/.+$/ { inhdr=1; print; next }
    inhdr && !seen_hdr++ {
      # эмулируем новый файл
      print "new file mode 100644"
      print "index 0000000..0000000"
      match($0,/diff --git a\/.+ b\/(.+)$/,m)
      print "--- /dev/null"
      print "+++ b/" m[1]
      inhdr=0
      next
    }
    { print }
  ' "$FIX.tmp1" > "$FIX"
else
  cp "$RAW" "$FIX"
fi

# Сначала пытаемся как git-format (Copy as git apply / git am)
if grep -qE '^From [0-9a-f]{40} ' "$FIX" && grep -q '^Subject:' "$FIX"; then
  if git am -3 < "$FIX"; then
    echo "[OK] Applied via git am"
  else
    git am --abort || true
    echo "[i] git am failed. Trying git apply…"
    if git apply --check --whitespace=fix "$FIX"; then
      git apply --whitespace=fix "$FIX"
      git add -A
      git commit -m "Apply patch from Codex" || true
    else
      echo "[i] git apply --check failed. Using --reject…"
      git apply --reject --whitespace=fix "$FIX"
      git add -A
      git commit -m "Apply patch from Codex (with rejects)" || true
    fi
  fi
else
  # Обычный unified diff
  if git apply --check --whitespace=fix "$FIX"; then
    git apply --whitespace=fix "$FIX"
    git add -A
    git commit -m "Apply patch from Codex" || true
  else
    echo "[i] git apply --check failed. Using --reject…"
    git apply --reject --whitespace=fix "$FIX"
    git add -A
    git commit -m "Apply patch from Codex (with rejects)" || true
  fi
fi

git pull --rebase --autostash || true
git push
echo "[DONE] Synced with GitHub."

rm -f "$RAW" "$FIX" "$FIX.tmp1" 2>/dev/null || true
