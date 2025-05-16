for file in docs/*.md; do
  case "$(basename "$file")" in
    _*|index.md|blog.md|docs.md) continue ;;
  esac

  awk '
    BEGIN { in_preamble=0; done_preamble=0 }
    NR==1 && $0 == "---" { in_preamble=1; next }
    in_preamble && $0 == "---" { in_preamble=0; done_preamble=1; next }
    in_preamble { next }
    done_preamble && $0 == "" { next }
    { done_preamble=0; print }
  ' "$file" > "$file.$$".tmp && mv "$file.$$".tmp "$file"
done
