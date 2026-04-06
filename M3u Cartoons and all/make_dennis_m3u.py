import re
import sys
import xml.etree.ElementTree as ET
from urllib.parse import quote
from urllib.request import urlopen


ITEM_ID = "dennis-the-menace-and-gnasher-the-complete-tv-series-1996-98"
BASE_ITEM_URL = f"https://archive.org/download/{ITEM_ID}"
FILES_XML_URL = f"{BASE_ITEM_URL}/{ITEM_ID}_files.xml"


def fetch_files_xml(url: str) -> str:
    with urlopen(url) as response:
        return response.read().decode("utf-8", errors="replace")


def parse_mp4_files(xml_text: str):
    root = ET.fromstring(xml_text)
    files = []
    for file_el in root.findall(".//file"):
        name = file_el.get("name", "")
        if name.lower().endswith(".mp4"):
            files.append(name)
    return files


def season_episode_key(filename: str):
    match = re.search(r"S(\d{2})E(\d{2})", filename, re.IGNORECASE)
    if not match:
        return (999, 999, filename.lower())
    return (int(match.group(1)), int(match.group(2)), filename.lower())


def build_url(filename: str) -> str:
    return f"{BASE_ITEM_URL}/{quote(filename)}"


def season_label(filename: str) -> str:
    match = re.search(r"S(\d{2})E(\d{2})", filename, re.IGNORECASE)
    if not match:
        return "Unknown Season"
    return f"Dennis the Menace and Gnasher Season {int(match.group(1))}"


def make_m3u_lines(filenames):
    lines = ["#EXTM3U"]
    for name in filenames:
        group = season_label(name)
        lines.append(
            "#EXTINF:-1 "
            f"group-title=\"{group}\" "
            "tvg-language=\"ENG\" "
            "language=\"ENG\" "
            "tvg-logo=\"https://iili.io/BxlIE9s.png\","
            f"{name}"
        )
        lines.append(build_url(name))
    return "\n".join(lines) + "\n"


def main(output_path: str):
    xml_text = fetch_files_xml(FILES_XML_URL)
    mp4_files = parse_mp4_files(xml_text)
    mp4_files.sort(key=season_episode_key)
    content = make_m3u_lines(mp4_files)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"Wrote {len(mp4_files)} entries to {output_path}")


if __name__ == "__main__":
    output = sys.argv[1] if len(sys.argv) > 1 else "dennis-the-menace.m3u"
    main(output)
