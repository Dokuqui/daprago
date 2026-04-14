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
    return name.replace('"', "").strip()


def extract_write_tables(sql: str):
    writes = []
    for pattern in WRITE_PATTERNS:
        match = re.search(pattern, sql, flags=re.IGNORECASE)
        if match:
            writes.append(normalize_table_name(match.group(1)))
    return list(set(writes))


@app.post("/parser")
def parser_sql():
    body = request.get_json(force=True)
    sql = body.get("sql", "")

    if not sql or not isinstance(sql, str):
        return jsonify({"error": "sql is required"}), 400

    try:
        parser = Parser(sql)
        reads = [normalize_table_name(t) for t in (parser.tables or [])]
        writes = extract_write_tables(sql)

        reads = [t for t in reads if t not in writes]

        return jsonify(
            {
                "query_hash": hashlib.md5(sql.encode("utf-8")).hexdigest(),
                "reads_tables": list(set(reads)),
                "writes_tables": list(set(writes)),
            }
        ), 200
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
