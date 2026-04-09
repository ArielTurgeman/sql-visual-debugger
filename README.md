# SQL Debugger MVP

An interactive SQL debugger that helps users understand how queries execute step-by-step through visual transformations.

---

## 🚀 Features

### Supported SQL
- `SELECT`
- `FROM`
- `INNER JOIN`
- `WHERE`
- `GROUP BY`
- `HAVING`
- `ORDER BY`
- `LIMIT`

---

## 🧠 Interactive Learning Experience

### 🔗 JOIN
- Click cells to trace matches
- Visual highlighting:
  - Selected cell → yellow
  - Selected row → green
  - Matching row → blue

---

### 🔍 WHERE / HAVING
- Removed rows are:
  - Highlighted in red
  - Strikethrough applied
- Clear visualization of filtering impact

---

### 📊 GROUP BY
- Click a grouped row to see:
  - All original rows that formed the group
- Aggregations (COUNT, SUM, AVG, etc.) are:
  - Clearly labeled
  - Visually connected to source columns

---

### 📈 ORDER BY
- Column used for ordering is highlighted
- Makes sorting logic immediately visible

---

## 🧾 SQL Flow Visualization

Query execution is broken into steps:
