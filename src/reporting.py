"""Helpers to turn Spark results into Markdown tables for REPORT.md."""


def to_markdown(df, n=50):
    """First n rows of a Spark DataFrame as a Markdown table (printable)."""
    rows = df.limit(n).collect()
    cols = df.columns
    lines = ["| " + " | ".join(cols) + " |",
             "|" + "|".join("---" for _ in cols) + "|"]
    for r in rows:
        lines.append("| " + " | ".join("" if v is None else str(v) for v in r) + " |")
    return "\n".join(lines)


def show_md(df, n=50):
    print(to_markdown(df, n))
