import re
from pathlib import Path

VALIDATED_OK = Path("./files/validations/ok")

CHORD_PATTERN = re.compile(r"\b([A-G][#b]?m?(maj7)?(sus2|sus4)?7?)\b")

def remove_chords(text: str) -> str:
    """Remove chords like A, Am, C#, Fmaj7, etc."""
    cleaned = CHORD_PATTERN.sub("", text)
    # Remove multiple spaces created by deleting chords
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned.strip()


def process_lyrics():
    for file in VALIDATED_OK.rglob("*.txt"):
        original = file.read_text(encoding="utf-8", errors="ignore")
        cleaned = remove_chords(original)

        # Crear archivo: la_llave_lyrics.txt
        output_path = file.with_name(file.stem + "_lyrics.txt")

        output_path.write_text(cleaned, encoding="utf-8")
        print("Created:", output_path)


if __name__ == "__main__":
    process_lyrics()
