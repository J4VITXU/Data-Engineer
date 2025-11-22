from pathlib import Path

OUTPUTS = {
    "songs": Path("./files/songs"),
    "cleaned": Path("./files/cleaned"),
    "validated_ok": Path("./files/validations/ok"),
    "validated_ko": Path("./files/validations/ko"),
}

def count_files():
    results = {}
    for name, path in OUTPUTS.items():
        if path.exists():
            results[name] = sum(1 for _ in path.rglob("*.txt"))
        else:
            results[name] = 0

    return results


if __name__ == "__main__":
    stats = count_files()
    print("Results:")
    for k, v in stats.items():
        print(f"{k}: {v} files")
