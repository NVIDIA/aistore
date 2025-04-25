# CLI Help ('--help') Style Guide

This document (briefly) outlines the style guidelines for consistent inline help formatting across all CLI commands.

---

## 1. Header for examples
- Use **`e.g.:`**  not `Examples:` or `Example:`

---

## 2. Quoting commands
- Wrap CLI examples inside **single quotes**: `'...'`

---

## 3. Ending example lines
- Example descriptions must end with a **semicolon `;`**

---

## 4. Format for each example line
```
'<command>'<TAB>- <short description>;
```

- Use the tab character (`\t`) to separate the command and its description.
- Keep it on a **single line** if possible.
- For long descriptions, break cleanly onto the next line and align visually under the text.

---

## 5. Multi-line descriptions
- If an example’s description is too long, break it manually after a comma or natural pause.
- Indent the continuation line to align under the description start.

---

## 6. General phrasing guidelines
- Use **active voice**: "download", "list", "show", etc.
- Use **imperative mood**: not "you can download...", just "download...".
- Always describe actions in the order: **[action] [what] [from where] [optional behavior]**.

---

# Example Template

```go
const cmdUsage = "Brief description.\n" +
    indent1 + "e.g.:\n" +
    indent1 + "\t- 'ais <command> <arg1> <arg2>'\t- short description;\n" +
    indent1 + "\t- 'ais <command> <arg1> --flag'\t- another description;"
```
