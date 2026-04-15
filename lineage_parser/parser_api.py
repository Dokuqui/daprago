import re

from flask import Flask, request, jsonify
import hashlib
from sql_metadata import Parser

app = Flask(__name__)

WRITE_PATTERNS = [
    r"^\s*INSERT\s+INTO\s+([a-zA-Z0-9_.\"]+)",
    r"^\s*UPDATE\s+([a-zA-Z0-9_.\"]+)",
    r"^\s*CREATE\s+TABLE\s+([a-zA-Z0-9_.\"]+)\s+AS",
    r"^\s*MERGE\s+INTO\s+([a-zA-Z0-9_.\"]+)",
]


def normalize_table_name(name: str) -> str:
    """
    Normalize table names:
    - remove quotes/backticks
    - trim spaces
    - reduce db.schema.table -> schema.table
    - keep schema.table when possible
    """
    if not name:
        return ""

    n = name.strip().replace('"', "").replace("`", "")
    n = re.sub(r"\s+", "", n)

    parts = n.split(".")
    if len(parts) >= 2:
        return f"{parts[-2]}.{parts[-1]}"
    return f"public.{parts[0]}" if parts and parts[0] else ""


def extract_write_tables(sql: str):
    writes = []
    for pattern in WRITE_PATTERNS:
        match = re.search(pattern, sql, flags=re.IGNORECASE)
        if match:
            writes.append(normalize_table_name(match.group(1)))
    return list(set(writes))


def remove_cte_aliases(sql: str, reads: list[str]) -> list[str]:
    """
    Remove CTE aliases from reads.
    Example:
      WITH recent_orders AS (...) SELECT * FROM recent_orders
    recent_orders is not a physical table.
    """
    cte_aliases = set()
    for alias in re.findall(
        r"(?:WITH|,)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s*\(", sql, flags=re.IGNORECASE
    ):
        cte_aliases.add(alias.lower())
        cte_aliases.add(f"public.{alias.lower()}")

    output = []
    for t in reads:
        if t.lower() not in cte_aliases:
            output.append(t)
    return output


def parse_sql_lineage(sql: str) -> dict:
    raw_reads = []

    try:
        parser = Parser(sql)
        raw_reads = parser.tables or []
    except ValueError as e:
        if "Not supported query type" in str(e):
            sql_upper = sql.strip().upper()
            if sql_upper.startswith("MERGE"):
                source_match = re.search(
                    r"USING\s+([a-zA-Z0-9_.\"]+)", sql, re.IGNORECASE
                )
                if source_match:
                    raw_reads.append(source_match.group(1))
        else:
            raise e

    reads = [normalize_table_name(t) for t in raw_reads if t]

    writes = extract_write_tables(sql)

    reads = remove_cte_aliases(sql, reads)

    read_set: set[str] = set([r for r in reads if r])
    write_set: set[str] = set([w for w in writes if w])

    read_set = read_set - write_set

    return {
        "query_hash": hashlib.md5(sql.encode("utf-8")).hexdigest(),
        "reads_tables": sorted(list(read_set)),
        "writes_tables": sorted(list(write_set)),
    }


@app.post("/parser")
def parser_sql():
    body = request.get_json(force=True)
    sql = body.get("sql", "")

    if not sql or not isinstance(sql, str):
        return jsonify({"error": "sql is required"}), 400

    try:
        parsed = parse_sql_lineage(sql)
        return jsonify(parsed), 200
    except Exception as e:
        return jsonify(
            {
                "query_hash": hashlib.md5(sql.encode("utf-8")).hexdigest(),
                "reads_tables": [],
                "writes_tables": [],
                "parse_error": str(e),
            }
        ), 200


@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8090)
