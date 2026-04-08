import json
import os
import queue
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
import tkinter as tk
from bs4 import BeautifulSoup
from tkinter import ttk


BASE_URL = "https://www.tokyoinsider.com"
ANIME_LIST_URL = f"{BASE_URL}/anime/list"
OUTPUT_FILE = "tokyoinsider.json"


@dataclass
class DdlCandidate:
    url: str
    size_mb: float
    label: str


def fetch_html(
    url: str,
    session: requests.Session,
    retries: int = 2,
    backoff: float = 1.0,
) -> BeautifulSoup:
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except requests.RequestException as error:
            last_error = error
            if attempt < retries:
                time.sleep(backoff)
    raise last_error if last_error else requests.RequestException("Unknown request error")


def is_movie_title(title: str) -> bool:
    title_lower = title.lower()
    return "movie" in title_lower or "the movie" in title_lower or "ova" in title_lower


def extract_anime_links(soup: BeautifulSoup) -> List[Tuple[str, str]]:
    links: List[Tuple[str, str]] = []
    seen: set = set()
    for section_header in soup.select("div.c_h1b"):
        header_text = " ".join(section_header.get_text(strip=True).split())
        if not header_text:
            continue
        for sibling in section_header.find_next_siblings():
            if "c_h1b" in sibling.get("class", []):
                break
            if not sibling.name == "div":
                continue
            if not any(cls in sibling.get("class", []) for cls in ("c_h2", "c_h2b")):
                continue
            anchor = sibling.find("a", href=True)
            if not anchor:
                continue
            href = anchor.get("href")
            text = " ".join(anchor.get_text(strip=True).split())
            if not href or href == "/anime/list" or "/episode/" in href:
                continue
            full_url = f"{BASE_URL}{href}"
            if full_url in seen:
                continue
            seen.add(full_url)
            links.append((full_url, text or href))
    return links


def extract_episode_links(soup: BeautifulSoup) -> List[Tuple[int, str]]:
    episodes: Dict[int, str] = {}

    downloads_header = soup.find("div", class_="c_h1b", string=re.compile(r"Downloads", re.IGNORECASE))
    if not downloads_header:
        return []

    for sibling in downloads_header.find_next_siblings():
        if "c_h1b" in sibling.get("class", []):
            break
        anchor = sibling.select_one("a.download-link")
        if not anchor:
            continue
        text = anchor.get_text(" ", strip=True).lower()
        if "episode" not in text:
            continue
        href = anchor.get("href")
        if not href:
            continue
        match = re.search(r"/episode/(\d+)", href)
        if not match:
            continue
        episode_number = int(match.group(1))
        full_url = f"{BASE_URL}{href}" if href.startswith("/") else href
        episodes[episode_number] = full_url

    return sorted(episodes.items(), key=lambda item: item[0])


def parse_size_mb(text: str) -> Optional[float]:
    size_match = re.search(r"Size:\s*([0-9.]+)\s*MB", text, re.IGNORECASE)
    if not size_match:
        return None
    return float(size_match.group(1))


def parse_uploader(text: str) -> Optional[str]:
    uploader_match = re.search(r"Uploader:\s*([A-Za-z0-9_\- ]+)", text, re.IGNORECASE)
    if not uploader_match:
        return None
    return uploader_match.group(1).strip()


def extract_candidates(soup: BeautifulSoup) -> List[DdlCandidate]:
    candidates: List[DdlCandidate] = []
    for block in soup.select("div.c_h2, div.c_h2b"):
        if not block.select_one("span.lang_en"):
            continue
        link = block.select_one("a[href^='https://']")
        finfo = block.select_one("div.finfo")
        if not link or not finfo:
            continue
        url = link.get("href", "")
        label = link.get_text(strip=True)
        finfo_text = finfo.get_text(" ", strip=True)
        uploader = parse_uploader(finfo_text)
        if uploader and uploader.lower() == "jusenshi":
            continue
        if re.search(r"\braw\b", label, re.IGNORECASE) or re.search(r"raw", url, re.IGNORECASE):
            continue
        if not re.search(r"\.(mp4|mkv|avi)(\?|$)", url, re.IGNORECASE) and not re.search(
            r"\.(mp4|mkv|avi)\s*$", label, re.IGNORECASE
        ):
            continue
        size_mb = parse_size_mb(finfo_text)
        if size_mb is None:
            continue
        candidates.append(
            DdlCandidate(url=url, size_mb=size_mb, label=label)
        )
    return candidates


def pick_candidate(candidates: List[DdlCandidate]) -> Optional[DdlCandidate]:
    if not candidates:
        return None
    def format_rank(candidate: DdlCandidate) -> int:
        url = candidate.url.lower()
        label = candidate.label.lower()
        if ".mp4" in url or label.endswith(".mp4"):
            return 0
        if ".mkv" in url or label.endswith(".mkv"):
            return 1
        return 2

    preferred = [c for c in candidates if 90 <= c.size_mb <= 300]
    if preferred:
        return min(preferred, key=lambda c: (format_rank(c), c.size_mb))
    under_90 = [c for c in candidates if c.size_mb < 90]
    if under_90:
        return max(under_90, key=lambda c: (c.size_mb, -format_rank(c)))
    over_300 = [c for c in candidates if c.size_mb > 300]
    if over_300:
        return min(over_300, key=lambda c: (c.size_mb, format_rank(c)))
    return None


def extract_summary_and_genres(soup: BeautifulSoup) -> Tuple[Optional[str], List[str]]:
    summary = None
    genres: List[str] = []

    summary_cell = soup.find("td", string=re.compile(r"^Summary:\s*$", re.IGNORECASE))
    if summary_cell:
        summary_container = summary_cell.find_next_sibling("td")
        if summary_container:
            summary = " ".join(summary_container.get_text(" ", strip=True).split())

    if summary is None:
        summary_label = soup.find(string=re.compile(r"Summary", re.IGNORECASE))
        if summary_label and summary_label.parent:
            summary_container = summary_label.parent.find_next_sibling()
            if summary_container:
                summary = " ".join(summary_container.get_text(" ", strip=True).split())

    genres_cell = soup.find("td", string=re.compile(r"^Genres:\s*$", re.IGNORECASE))
    if genres_cell:
        genres_container = genres_cell.find_next_sibling("td")
        if genres_container:
            genres = [g.get_text(strip=True) for g in genres_container.select("a") if g.get_text(strip=True)]

    if not genres:
        genres_label = soup.find(string=re.compile(r"Genres", re.IGNORECASE))
        if genres_label and genres_label.parent:
            genres_container = genres_label.parent.find_next_sibling()
            if genres_container:
                genres = [g.get_text(strip=True) for g in genres_container.select("a") if g.get_text(strip=True)]

    return summary, genres


def write_json(payload: Dict, on_save: Optional[callable] = None) -> None:
    with open(OUTPUT_FILE, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    if on_save:
        on_save()


def load_existing_payload() -> Dict:
    if not os.path.exists(OUTPUT_FILE):
        return {"total_anime": 0, "items": []}
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        if isinstance(data, dict) and "items" in data:
            return {"total_anime": data.get("total_anime", 0), "items": data.get("items", [])}
    except (json.JSONDecodeError, OSError):
        pass
    return {"total_anime": 0, "items": []}


def scrape(
    on_event: Optional[callable] = None,
    cancel_event: Optional[threading.Event] = None,
    pause_event: Optional[threading.Event] = None,
) -> None:
    session = requests.Session()
    payload = load_existing_payload()
    all_results: List[Dict] = payload.get("items", [])

    def emit(event_type: str, **payload: Dict) -> None:
        if on_event:
            on_event({"type": event_type, **payload})

    def cancelled() -> bool:
        return cancel_event.is_set() if cancel_event else False

    def paused() -> bool:
        return pause_event.is_set() if pause_event else False

    def wait_if_paused() -> None:
        while paused() and not cancelled():
            emit("paused")
            time.sleep(0.2)

    list_soup = fetch_html(ANIME_LIST_URL, session)
    anime_links = extract_anime_links(list_soup)

    payload["total_anime"] = len(all_results)
    index_width = max(2, len(str(len(anime_links))))

    emit("anime_list", total=len(anime_links))

    resume_url = all_results[-1].get("source_url") if all_results else None
    start_index = 1
    if resume_url:
        for idx, (anime_url, _title) in enumerate(anime_links, start=1):
            if anime_url == resume_url:
                start_index = idx
                break

    for anime_index, (anime_url, title) in enumerate(anime_links, start=1):
        if anime_index < start_index:
            continue
        if cancelled():
            emit("cancelled")
            return
        wait_if_paused()
        anime_title = title.strip() or anime_url.rsplit("/", 1)[-1]
        if is_movie_title(anime_title):
            continue

        anime_soup = fetch_html(anime_url, session)
        episode_links = extract_episode_links(anime_soup)
        if len(episode_links) <= 2:
            emit("anime_skipped", title=anime_title, reason="No episode downloads found")
            continue

        summary, genres = extract_summary_and_genres(anime_soup)

        emit(
            "anime_start",
            title=anime_title,
            index=anime_index,
            total=len(anime_links),
            total_episodes=len(episode_links),
        )

        existing_entry = None
        if all_results and all_results[-1].get("source_url") == anime_url:
            existing_entry = all_results[-1]

        if existing_entry is None:
            anime_entry = {
                "Anime_Index": str(len(all_results) + 1).zfill(index_width),
                "anime": anime_title,
                "source_url": anime_url,
                "summary": summary,
                "genres": genres,
                "episodes": [],
            }
            all_results.append(anime_entry)
        else:
            anime_entry = existing_entry
            anime_entry["summary"] = summary
            anime_entry["genres"] = genres
            anime_entry["Anime_Index"] = anime_entry.get("Anime_Index") or str(len(all_results)).zfill(index_width)

        existing_episodes = {ep.get("episode") for ep in anime_entry.get("episodes", [])}

        for episode_index, (episode_number, episode_url) in enumerate(episode_links, start=1):
            if cancelled():
                emit("cancelled")
                return
            wait_if_paused()
            if episode_number in existing_episodes:
                continue
            emit(
                "episode_start",
                episode=episode_number,
                episode_index=episode_index,
                total_episodes=len(episode_links),
            )
            try:
                episode_soup = fetch_html(episode_url, session)
            except requests.RequestException:
                emit("episode_failed", episode=episode_number, url=episode_url)
                continue
            candidates = extract_candidates(episode_soup)
            selected = pick_candidate(candidates)
            if not selected:
                continue
            anime_entry["episodes"].append(
                {
                    "episode": episode_number,
                    "url": selected.url,
                    "size_mb": selected.size_mb,
                    "label": selected.label,
                }
            )
            payload["items"] = all_results
            payload["total_anime"] = len(all_results)
            write_json(
                payload,
                on_save=lambda: emit(
                    "saved",
                    timestamp=datetime.now().strftime("%H:%M:%S"),
                    saved_count=len(all_results),
                ),
            )

        if not anime_entry["episodes"]:
            if all_results and all_results[-1] is anime_entry:
                all_results.pop()
            continue

        emit("anime_done", title=anime_title, episodes=len(anime_entry["episodes"]))
        time.sleep(3)

    payload["items"] = all_results
    payload["total_anime"] = len(all_results)
    write_json(
        payload,
        on_save=lambda: emit(
            "saved",
            timestamp=datetime.now().strftime("%H:%M:%S"),
            saved_count=len(all_results),
        ),
    )


class ScraperApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("TokyoInsider Scraper")
        self.root.geometry("960x620")
        self.root.configure(bg="#0b0f14")

        self.cancel_event = threading.Event()
        self.pause_event = threading.Event()
        self.scraper_thread: Optional[threading.Thread] = None

        style = ttk.Style(self.root)
        style.theme_use("clam")
        style.configure("TFrame", background="#0b0f14")
        style.configure("TLabel", background="#0b0f14", foreground="#e8f1ff", font=("Segoe UI", 11))
        style.configure("Title.TLabel", font=("Segoe UI Semibold", 20), foreground="#7dd3fc")
        style.configure("Badge.TLabel", font=("Segoe UI Semibold", 10), foreground="#0b0f14", background="#34d399")
        style.configure("Status.TLabel", font=("Segoe UI", 11), foreground="#93c5fd")
        style.configure("TProgressbar", troughcolor="#111827", background="#38bdf8", thickness=14)
        style.configure(
            "Run.TButton",
            font=("Segoe UI Semibold", 10),
            foreground="#0b0f14",
            background="#34d399",
            padding=(14, 6),
            borderwidth=0,
        )
        style.map(
            "Run.TButton",
            background=[("active", "#2fb882"), ("disabled", "#1f2937")],
            foreground=[("disabled", "#6b7280")],
        )
        style.configure(
            "Stop.TButton",
            font=("Segoe UI Semibold", 10),
            foreground="#0b0f14",
            background="#f97316",
            padding=(14, 6),
            borderwidth=0,
        )
        style.map(
            "Stop.TButton",
            background=[("active", "#ea580c"), ("disabled", "#1f2937")],
            foreground=[("disabled", "#6b7280")],
        )

        header = ttk.Frame(self.root)
        header.pack(fill="x", padx=24, pady=(20, 12))
        ttk.Label(header, text="TokyoInsider Live Scraper", style="Title.TLabel").pack(side="left")
        self.badge = ttk.Label(header, text="IDLE", style="Badge.TLabel", padding=(10, 4))
        self.badge.pack(side="right")

        controls = ttk.Frame(self.root)
        controls.pack(fill="x", padx=24, pady=(0, 10))
        self.run_button = ttk.Button(controls, text="Run", command=self.start_scraper, style="Run.TButton")
        self.run_button.pack(side="left", padx=(0, 10))
        self.pause_button = ttk.Button(controls, text="Pause", command=self.toggle_pause, style="Run.TButton")
        self.pause_button.pack(side="left", padx=(0, 10))
        self.pause_button.state(["disabled"])
        self.stop_button = ttk.Button(controls, text="Stop", command=self.stop_scraper, style="Stop.TButton")
        self.stop_button.pack(side="left")
        self.stop_button.state(["disabled"])

        stats = ttk.Frame(self.root)
        stats.pack(fill="x", padx=24, pady=8)

        self.anime_label = ttk.Label(stats, text="Anime: —", style="Status.TLabel")
        self.anime_label.grid(row=0, column=0, sticky="w", padx=(0, 20))

        self.episode_label = ttk.Label(stats, text="Episode: —", style="Status.TLabel")
        self.episode_label.grid(row=0, column=1, sticky="w", padx=(0, 20))

        self.total_label = ttk.Label(stats, text="Total Episodes: —", style="Status.TLabel")
        self.total_label.grid(row=0, column=2, sticky="w")

        self.saved_label = ttk.Label(stats, text="Last Saved: —", style="Status.TLabel")
        self.saved_label.grid(row=1, column=0, sticky="w", pady=(8, 0))

        self.saved_count_label = ttk.Label(stats, text="Saved Anime: 0", style="Status.TLabel")
        self.saved_count_label.grid(row=1, column=1, sticky="w", pady=(8, 0))

        self.progress = ttk.Progressbar(self.root, orient="horizontal", mode="determinate")
        self.progress.pack(fill="x", padx=24, pady=(8, 12))

        log_frame = ttk.Frame(self.root)
        log_frame.pack(fill="both", expand=True, padx=24, pady=(0, 20))

        self.log = tk.Text(
            log_frame,
            height=18,
            bg="#0f172a",
            fg="#dbeafe",
            insertbackground="#dbeafe",
            relief="flat",
            font=("Consolas", 10),
        )
        self.log.pack(fill="both", expand=True, padx=8, pady=8)
        self.log.configure(state="disabled")

        self.event_queue: queue.Queue = queue.Queue()
        self.total_anime = 0
        self.current_total_episodes = 0

        self.root.after(200, self.process_events)

    def run_scraper(self) -> None:
        scrape(
            on_event=lambda event: self.event_queue.put(event),
            cancel_event=self.cancel_event,
            pause_event=self.pause_event,
        )
        self.event_queue.put({"type": "finished"})

    def start_scraper(self) -> None:
        if self.scraper_thread and self.scraper_thread.is_alive():
            return
        self.cancel_event.clear()
        self.anime_label.config(text="Anime: —")
        self.episode_label.config(text="Episode: —")
        self.total_label.config(text="Total Episodes: —")
        self.saved_label.config(text="Last Saved: —")
        self.progress.config(value=0)
        self.badge.config(text="RUNNING", background="#34d399")
        self.run_button.state(["disabled"])
        self.pause_button.state(["!disabled"])
        self.stop_button.state(["!disabled"])
        self.append_log("Starting scraper with resume enabled.")
        self.scraper_thread = threading.Thread(target=self.run_scraper, daemon=True)
        self.scraper_thread.start()

    def stop_scraper(self) -> None:
        if not self.scraper_thread or not self.scraper_thread.is_alive():
            return
        self.cancel_event.set()
        self.append_log("Stop requested. Finishing current step.")

    def toggle_pause(self) -> None:
        if not self.scraper_thread or not self.scraper_thread.is_alive():
            return
        if self.pause_event.is_set():
            self.pause_event.clear()
            self.pause_button.config(text="Pause")
            self.badge.config(text="RUNNING", background="#34d399")
            self.append_log("Resumed.")
        else:
            self.pause_event.set()
            self.pause_button.config(text="Resume")
            self.badge.config(text="PAUSED", background="#f59e0b")
            self.append_log("Paused.")

    def append_log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log.configure(state="normal")
        self.log.insert("end", f"[{timestamp}] {message}\n")
        self.log.see("end")
        self.log.configure(state="disabled")

    def process_events(self) -> None:
        while True:
            try:
                event = self.event_queue.get_nowait()
            except queue.Empty:
                break

            event_type = event.get("type")
            if event_type == "anime_list":
                self.total_anime = event.get("total", 0)
                self.append_log(f"Discovered {self.total_anime} anime entries.")
            elif event_type == "anime_start":
                title = event.get("title", "—")
                index = event.get("index", 0)
                total = event.get("total", 0)
                self.current_total_episodes = event.get("total_episodes", 0)
                self.anime_label.config(text=f"Anime: {title}")
                self.total_label.config(text=f"Total Episodes: {self.current_total_episodes}")
                self.progress.config(maximum=self.current_total_episodes, value=0)
                self.append_log(f"[{index}/{total}] Scraping {title}.")
            elif event_type == "episode_start":
                episode = event.get("episode", "—")
                episode_index = event.get("episode_index", 0)
                total_episodes = event.get("total_episodes", 0)
                self.episode_label.config(text=f"Episode: {episode}")
                self.progress.config(maximum=total_episodes, value=episode_index)
                self.append_log(f"Fetching episode {episode}.")
            elif event_type == "saved":
                timestamp = event.get("timestamp", "—")
                saved_count = event.get("saved_count")
                self.saved_label.config(text=f"Last Saved: {timestamp}")
                if saved_count is not None:
                    self.saved_count_label.config(text=f"Saved Anime: {saved_count}")
                self.append_log("Saved JSON output.")
            elif event_type == "anime_skipped":
                title = event.get("title", "—")
                reason = event.get("reason", "Unknown")
                self.append_log(f"Skipped {title}: {reason}.")
            elif event_type == "episode_failed":
                episode = event.get("episode", "—")
                url = event.get("url", "—")
                self.append_log(f"Episode {episode} failed after retries: {url}.")
            elif event_type == "anime_done":
                title = event.get("title", "—")
                episodes = event.get("episodes", 0)
                self.append_log(f"Finished {title} with {episodes} episodes saved.")
            elif event_type == "finished":
                self.badge.config(text="DONE", background="#22c55e")
                self.append_log("Scraping complete.")
                self.run_button.state(["!disabled"])
                self.pause_button.state(["disabled"])
                self.pause_button.config(text="Pause")
                self.stop_button.state(["disabled"])
            elif event_type == "cancelled":
                self.badge.config(text="STOPPED", background="#f97316")
                self.append_log("Scraping stopped by user.")
                self.run_button.state(["!disabled"])
                self.pause_button.state(["disabled"])
                self.pause_button.config(text="Pause")
                self.stop_button.state(["disabled"])
            elif event_type == "paused":
                if not self.pause_event.is_set():
                    continue

        self.root.after(200, self.process_events)


if __name__ == "__main__":
    root = tk.Tk()
    app = ScraperApp(root)
    root.mainloop()
