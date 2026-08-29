"""
Deterministic fixture extractor for Deriv Senior Data Engineering Assessment.
Extracts the embedded JSON, CSV, and JSONL datasets from task_instructions.md into data/
without modifying any content or values.
"""

from pathlib import Path
import re

# Human-readable justification:
# Extracts the exact embedded dataset text blocks from task_instructions.md deterministically
# into the /data folder to prevent any manual transcription error or silent value modification.

def extract_fixtures(instructions_path: Path, output_dir: Path) -> dict[str, int]:
    """Parse task_instructions.md and extract the 8 embedded fixture files."""
    content = instructions_path.read_text(encoding="utf-8")
    output_dir.mkdir(parents=True, exist_ok=True)

    pattern = re.compile(
        r'###\s+`([^`]+)`.*?'
        r'```(?:json|jsonl|csv)\s*\n'
        r'(.*?)'
        r'\n```',
        re.DOTALL
    )

    extracted_counts = {}
    for match in pattern.finditer(content):
        filename = match.group(1).strip()
        data = match.group(2)
        out_file = output_dir / filename
        out_file.write_text(data + "\n", encoding="utf-8")
        line_count = len([line for line in data.splitlines() if line.strip()])
        extracted_counts[filename] = line_count
        print(f"Extracted {filename} ({len(data)} bytes, {line_count} non-empty lines)")

    return extracted_counts

if __name__ == "__main__":
    repo_root = Path(__file__).resolve().parent.parent
    instructions_file = repo_root / "task_instructions.md"
    data_dir = repo_root / "data"
    counts = extract_fixtures(instructions_file, data_dir)
    print(f"Total fixtures extracted: {len(counts)}")
