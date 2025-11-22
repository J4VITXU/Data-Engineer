from pathlib import Path
import re
from collections import Counter

LYRICS_DIR = Path("./files/validations/ok")
OUTPUT_DIR = Path("./files/insights")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

STOPWORDS = {"de", "la", "el", "que", "y", "a", "en", "un", "una", "me", "te",
             "tu", "su", "los", "las", "con", "por", "para", "mi", "si"}

WORD_RE = re.compile(r"[a-záéíóúüñ]+")

def extract_words(text: str):
    words = WORD_RE.findall(text.lower())
    return [w for w in words if w not in STOPWORDS]


def process_insights():
    global_words = []

    for artist_dir in LYRICS_DIR.iterdir():
        if not artist_dir.is_dir():
            continue

        artist_words = []
        merged_text = ""

        for file in artist_dir.glob("*_lyrics.txt"):
            text = file.read_text(encoding="utf-8", errors="ignore")
            merged_text += text + "\n"
            artist_words.extend(extract_words(text))

        if not merged_text.strip():
            continue
        
        # Crear archivo por artista
        merged_file = OUTPUT_DIR / f"{artist_dir.name}_lyrics_full.txt"
        merged_file.write_text(merged_text, encoding="utf-8")

        # Top 10 palabras del artista
        top10 = Counter(artist_words).most_common(10)
        print(f"Top 10 words for {artist_dir.name}: {top10}")

        global_words.extend(artist_words)

    # Top 20 globales
    top20 = Counter(global_words).most_common(20)
    print("\n==== GLOBAL TOP 20 WORDS ====")
    print(top20)

    # Guardar los resultados
    (OUTPUT_DIR / "top20_global.txt").write_text(
        "\n".join([f"{w}: {c}" for w, c in top20]),
        encoding="utf-8"
    )


if __name__ == "__main__":
    process_insights()
