import json
from pathlib import Path


INPUT_JSON = Path("tokyoinsider.json")
OUTPUT_M3U = Path("tokyoinsider.m3u8")


def load_payload() -> dict:
    if not INPUT_JSON.exists():
        raise FileNotFoundError(f"Missing {INPUT_JSON}")
    with INPUT_JSON.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def build_m3u(payload: dict) -> str:
    lines = ["#EXTM3U"]
    items = payload.get("items", [])

    for anime in items:
        anime_name = anime.get("anime", "Unknown Anime")
        anime_index = anime.get("Anime_Index", "")
        episodes = anime.get("episodes", [])

        for episode in episodes:
            ep_num = episode.get("episode")
            url = episode.get("url")
            if not url:
                continue
            display = f"{anime_name} - Ep {ep_num}" if ep_num is not None else anime_name
            extinf = (
                f"#EXTINF:-1 "
                f"group-title=\"{anime_name}\" "
                f"tvg-name=\"{anime_name}\" "
                f"tvg-id=\"{anime_index}\""
                f",{display}"
            )
            lines.append(extinf)
            lines.append(url)

    return "\n".join(lines) + "\n"


def main() -> None:
    payload = load_payload()
    m3u_text = build_m3u(payload)
    OUTPUT_M3U.write_text(m3u_text, encoding="utf-8")


if __name__ == "__main__":
    main()
