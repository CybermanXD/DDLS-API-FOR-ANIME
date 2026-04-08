import json
import queue
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import tkinter as tk
from bs4 import BeautifulSoup
from tkinter import messagebox, ttk


BASE_URL = "https://www.tokyoinsider.com"
OUTPUT_JSON = Path("tokyoinsider_manual.json")
OUTPUT_M3U = Path("tokyoinsider_manual.m3u8")
LOG_FILE = Path("logs.txt")


@dataclass
class DdlCandidate:
    url: str
    size_mb: float
    label: str


@dataclass
class AnimeTask:
    url: str
    title: str = ""
    status: str = "Queued"
    total_episodes: int = 0
    saved_episodes: int = 0
    last_updated: str = "—"
    selected: bool = False


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
        if not re.search(r"\.(mp4|mkv|avi)(\?|$)", url, re.IGNORECASE) and not re.search(
            r"\.(mp4|mkv|avi)\s*$", label, re.IGNORECASE
        ):
            continue
        size_mb = parse_size_mb(finfo.get_text(" ", strip=True))
        if size_mb is None:
            continue
        candidates.append(DdlCandidate(url=url, size_mb=size_mb, label=label))
    return candidates


def select_candidate_with_reason(soup: BeautifulSoup) -> Tuple[Optional[DdlCandidate], Optional[str]]:
    candidates: List[DdlCandidate] = []
    has_english = False
    has_english_format = False
    raw_filtered = False
    uploader_filtered = False

    for block in soup.select("div.c_h2, div.c_h2b"):
        if not block.select_one("span.lang_en"):
            continue
        has_english = True
        link = block.select_one("a[href^='https://']")
        finfo = block.select_one("div.finfo")
        if not link or not finfo:
            continue
        url = link.get("href", "")
        label = link.get_text(strip=True)
        finfo_text = finfo.get_text(" ", strip=True)
        uploader = parse_uploader(finfo_text)
        if uploader and uploader.lower() == "jusenshi":
            uploader_filtered = True
            continue
        if re.search(r"\braws?\b", label, re.IGNORECASE) or re.search(r"raws?", url, re.IGNORECASE):
            raw_filtered = True
            continue
        is_format = re.search(r"\.(mp4|mkv|avi)(\?|$)", url, re.IGNORECASE) or re.search(
            r"\.(mp4|mkv|avi)\s*$", label, re.IGNORECASE
        )
        if not is_format:
            continue
        has_english_format = True
        size_mb = parse_size_mb(finfo_text)
        if size_mb is None:
            continue
        candidates.append(DdlCandidate(url=url, size_mb=size_mb, label=label))

    if not has_english:
        return None, "No English links"
    if not has_english_format:
        return None, "No MP4/MKV/AVI English links"
    if uploader_filtered and not candidates:
        return None, "Uploader Jusenshi excluded"
    if raw_filtered and not candidates:
        return None, "RAW title excluded"

    selected = pick_candidate(candidates)
    if not selected:
        return None, "No links matching size rules"
    return selected, None


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


def extract_poster_url(soup: BeautifulSoup) -> Optional[str]:
    img = soup.select_one("img.a_img")
    if not img:
        return None
    src = img.get("src", "")
    if not src:
        return None
    if src.startswith("/"):
        return f"{BASE_URL}{src}"
    return src


def load_payload() -> Dict:
    if OUTPUT_JSON.exists():
        try:
            return json.loads(OUTPUT_JSON.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    return {"total_anime": 0, "items": []}


def write_payload(payload: Dict) -> None:
    OUTPUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def build_m3u(payload: Dict) -> str:
    lines = ["#EXTM3U"]
    items = sorted(payload.get("items", []), key=lambda item: item.get("anime", ""))
    for anime in items:
        anime_name = anime.get("anime", "Unknown Anime")
        clean_name = anime_name.replace("_", " ")
        anime_index = anime.get("Anime_Index", "")
        poster_url = anime.get("poster_url", "")
        for episode in anime.get("episodes", []):
            ep_num = episode.get("episode")
            url = episode.get("url")
            if not url:
                continue
            display = (
                f"{anime_index} {clean_name} - Ep {ep_num}" if ep_num is not None else f"{anime_index} {clean_name}"
            )
            extinf = (
                f"#EXTINF:-1 "
                f"group-title=\"{clean_name}\" "
                f"tvg-name=\"{clean_name}\" "
                f"tvg-id=\"{anime_index}\" "
                f"tvg-logo=\"{poster_url}\""
                f",{display}"
            )
            lines.append(extinf)
            lines.append(url)
    return "\n".join(lines) + "\n"


class ManualScraperApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("TokyoInsider Manual Scraper")
        self.root.geometry("1040x680")
        self.root.configure(bg="#0b0f14")

        self.cancel_event = threading.Event()
        self.pause_event = threading.Event()
        self.worker_thread: Optional[threading.Thread] = None
        self.event_queue: queue.Queue = queue.Queue()
        self.tasks: List[AnimeTask] = []
        self.needs_refresh = True

        self.payload = load_payload()
        self.load_tasks_from_payload()

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
        header.pack(fill="x", padx=24, pady=(20, 10))
        ttk.Label(header, text="TokyoInsider Manual Scraper", style="Title.TLabel").pack(side="left")
        self.badge = ttk.Label(header, text="IDLE", style="Badge.TLabel", padding=(10, 4))
        self.badge.pack(side="right")

        input_frame = ttk.Frame(self.root)
        input_frame.pack(fill="x", padx=24, pady=(0, 10))
        ttk.Label(input_frame, text="Anime URL:", style="Status.TLabel").pack(side="left")
        self.url_entry = tk.Entry(input_frame, bg="#0f172a", fg="#dbeafe", insertbackground="#dbeafe")
        self.url_entry.pack(side="left", fill="x", expand=True, padx=(10, 10))
        ttk.Button(input_frame, text="Add", command=self.add_task, style="Run.TButton").pack(side="left")

        controls = ttk.Frame(self.root)
        controls.pack(fill="x", padx=24, pady=(0, 10))
        self.run_button = ttk.Button(controls, text="Run", command=self.start_scrape, style="Run.TButton")
        self.run_button.pack(side="left", padx=(0, 10))
        self.pause_button = ttk.Button(controls, text="Pause", command=self.toggle_pause, style="Run.TButton")
        self.pause_button.pack(side="left", padx=(0, 10))
        self.pause_button.state(["disabled"])
        self.stop_button = ttk.Button(controls, text="Stop", command=self.stop_scrape, style="Stop.TButton")
        self.stop_button.pack(side="left")
        self.stop_button.state(["disabled"])
        ttk.Button(controls, text="Re-fetch Selected", command=self.refetch_selected, style="Run.TButton").pack(
            side="left", padx=(10, 0)
        )
        ttk.Button(controls, text="Re-fetch Posters", command=self.refetch_posters, style="Run.TButton").pack(
            side="left", padx=(10, 0)
        )
        ttk.Button(controls, text="Delete Selected", command=self.delete_selected, style="Stop.TButton").pack(
            side="left", padx=(10, 0)
        )

        stats = ttk.Frame(self.root)
        stats.pack(fill="x", padx=24, pady=(0, 10))
        self.status_label = ttk.Label(stats, text="Queued: 0 | Done: 0", style="Status.TLabel")
        self.status_label.pack(side="left")
        self.saved_label = ttk.Label(
            stats,
            text=f"Saved Anime: {len(self.payload.get('items', []))}",
            style="Status.TLabel",
        )
        self.saved_label.pack(side="right")

        table_frame = ttk.Frame(self.root)
        table_frame.pack(fill="both", expand=True, padx=24, pady=(0, 10))

        header = tk.Frame(table_frame, bg="#0b0f14")
        header.pack(fill="x")
        tk.Label(header, text="Pick", width=6, bg="#0b0f14", fg="#93c5fd").pack(side="left")
        tk.Label(header, text="Anime", width=50, anchor="w", bg="#0b0f14", fg="#93c5fd").pack(side="left")
        tk.Label(header, text="Status", width=12, anchor="w", bg="#0b0f14", fg="#93c5fd").pack(side="left")
        tk.Label(header, text="Episodes", width=12, anchor="center", bg="#0b0f14", fg="#93c5fd").pack(side="left")
        tk.Label(header, text="Last Updated", width=14, anchor="center", bg="#0b0f14", fg="#93c5fd").pack(
            side="left"
        )

        self.canvas = tk.Canvas(table_frame, bg="#0b0f14", highlightthickness=0)
        self.scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        self.scrollbar.pack(side="right", fill="y")
        self.canvas.pack(side="left", fill="both", expand=True)

        self.rows_frame = tk.Frame(self.canvas, bg="#0b0f14")
        self.canvas_window = self.canvas.create_window((0, 0), window=self.rows_frame, anchor="nw")
        self.rows_frame.bind(
            "<Configure>",
            lambda _event: self.canvas.configure(scrollregion=self.canvas.bbox("all")),
        )
        self.canvas.bind(
            "<Configure>",
            lambda event: self.canvas.itemconfigure(self.canvas_window, width=event.width),
        )
        self.canvas.bind("<Enter>", lambda _event: self._bind_mousewheel())
        self.canvas.bind("<Leave>", lambda _event: self._unbind_mousewheel())

        log_frame = ttk.Frame(self.root)
        log_frame.pack(fill="both", expand=True, padx=24, pady=(0, 20))
        self.log = tk.Text(
            log_frame,
            height=8,
            bg="#0f172a",
            fg="#dbeafe",
            insertbackground="#dbeafe",
            relief="flat",
            font=("Consolas", 10),
        )
        self.log.pack(fill="both", expand=True, padx=8, pady=8)
        self.log.configure(state="disabled")

        self.root.after(200, self.process_events)
        self.refresh_tree()

    def add_task(self) -> None:
        url = self.url_entry.get().strip()
        if not url:
            return
        task = AnimeTask(url=url)
        self.tasks.append(task)
        self.url_entry.delete(0, "end")
        self.append_log(f"Queued {url}.")
        self.needs_refresh = True
        self.refresh_tree()

    def start_scrape(self) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            return
        if not self.tasks:
            self.append_log("No anime URLs queued.")
            return
        self.cancel_event.clear()
        self.pause_event.clear()
        self.badge.config(text="RUNNING", background="#34d399")
        self.run_button.state(["disabled"])
        self.pause_button.state(["!disabled"])
        self.stop_button.state(["!disabled"])
        self.worker_thread = threading.Thread(target=self.run_worker, daemon=True)
        self.worker_thread.start()

    def stop_scrape(self) -> None:
        if not self.worker_thread or not self.worker_thread.is_alive():
            return
        self.cancel_event.set()
        self.append_log("Stop requested. Finishing current step.")

    def toggle_pause(self) -> None:
        if not self.worker_thread or not self.worker_thread.is_alive():
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

    def refetch_selected(self) -> None:
        selected_tasks = [task for task in self.tasks if task.selected]
        if not selected_tasks:
            return
        for task in selected_tasks:
            task.status = "Queued"
            self.append_log(f"Re-fetch queued for {task.url}.")
        self.needs_refresh = True
        self.refresh_tree()

    def delete_selected(self) -> None:
        selected_tasks = [task for task in self.tasks if task.selected]
        if not selected_tasks:
            return
        confirm = messagebox.askyesno(
            "Delete Selected",
            "Delete selected anime from dashboard and JSON?",
        )
        if not confirm:
            return
        delete_urls = {task.url for task in selected_tasks}
        self.tasks = [task for task in self.tasks if task.url not in delete_urls]
        items = self.payload.get("items", [])
        self.payload["items"] = [item for item in items if item.get("source_url") not in delete_urls]
        self.save_payload()
        OUTPUT_M3U.write_text(build_m3u(self.payload), encoding="utf-8")
        self.append_log(f"Deleted {len(delete_urls)} anime entries.")
        self.needs_refresh = True
        self.refresh_tree()

    def refetch_posters(self) -> None:
        selected_tasks = [task for task in self.tasks if task.selected]
        if not selected_tasks:
            return
        session = requests.Session()
        for task in selected_tasks:
            try:
                anime_soup = fetch_html(task.url, session)
            except requests.RequestException:
                self.append_log(f"Poster fetch failed for {task.url}.")
                continue
            poster_url = extract_poster_url(anime_soup)
            if not poster_url:
                self.append_log(f"No poster found for {task.url}.")
                continue
            for entry in self.payload.get("items", []):
                if entry.get("source_url") == task.url:
                    entry["poster_url"] = poster_url
                    break
            self.append_log(f"Poster updated for {task.title or task.url}.")
        self.save_payload()
        OUTPUT_M3U.write_text(build_m3u(self.payload), encoding="utf-8")

    def run_worker(self) -> None:
        session = requests.Session()
        for task in self.tasks:
            if self.cancel_event.is_set():
                break
            if task.status == "Done":
                continue
            while self.pause_event.is_set() and not self.cancel_event.is_set():
                time.sleep(0.2)
            task.status = "Running"
            self.event_queue.put({"type": "task_start", "task": task})

            try:
                anime_soup = fetch_html(task.url, session)
            except requests.RequestException:
                task.status = "Failed"
                self.event_queue.put({"type": "task_failed", "task": task})
                continue

            title = anime_soup.find("div", class_="c_h1")
            anime_title = title.get_text(" ", strip=True) if title else task.url.rsplit("/", 1)[-1]
            task.title = anime_title.replace("Download ", "").strip()

            if is_movie_title(task.title):
                task.status = "Skipped"
                self.event_queue.put({"type": "task_skipped", "task": task})
                continue

            episode_links = extract_episode_links(anime_soup)
            if not episode_links:
                task.status = "Skipped"
                self.event_queue.put({"type": "task_skipped", "task": task})
                continue

            summary, genres = extract_summary_and_genres(anime_soup)
            poster_url = extract_poster_url(anime_soup)
            task.total_episodes = len(episode_links)

            anime_entry = self.find_or_create_entry(task, summary, genres)
            anime_entry["total_episodes"] = len(episode_links)
            if poster_url:
                anime_entry["poster_url"] = poster_url
            existing_episodes = {ep.get("episode") for ep in anime_entry.get("episodes", [])}

            if len(existing_episodes) >= len(episode_links):
                task.saved_episodes = len(existing_episodes)
                task.total_episodes = len(episode_links)
                task.status = "Done"
                task.last_updated = datetime.now().strftime("%H:%M:%S")
                self.event_queue.put({"type": "task_done", "task": task})
                continue

            for episode_number, episode_url in episode_links:
                if self.cancel_event.is_set():
                    break
                while self.pause_event.is_set() and not self.cancel_event.is_set():
                    time.sleep(0.2)
                if episode_number in existing_episodes:
                    continue
                try:
                    episode_soup = fetch_html(episode_url, session)
                except requests.RequestException:
                    self.event_queue.put({"type": "episode_failed", "task": task, "episode": episode_number})
                    continue
                candidate, reason = select_candidate_with_reason(episode_soup)
                if not candidate:
                    self.event_queue.put(
                        {
                            "type": "episode_skipped",
                            "task": task,
                            "episode": episode_number,
                            "reason": reason or "No candidate",
                        }
                    )
                    continue
                anime_entry["episodes"].append(
                    {
                        "episode": episode_number,
                        "url": candidate.url,
                        "size_mb": candidate.size_mb,
                        "label": candidate.label,
                    }
                )
                task.saved_episodes = len(anime_entry["episodes"])
                task.last_updated = datetime.now().strftime("%H:%M:%S")
                self.save_payload()
                self.event_queue.put({"type": "episode_saved", "task": task})

            task.status = "Done"
            self.event_queue.put({"type": "task_done", "task": task})
            time.sleep(3)

        self.save_payload()
        OUTPUT_M3U.write_text(build_m3u(self.payload), encoding="utf-8")
        self.event_queue.put({"type": "finished"})

    def find_or_create_entry(self, task: AnimeTask, summary: Optional[str], genres: List[str]) -> Dict:
        items = self.payload.setdefault("items", [])
        for entry in items:
            if entry.get("source_url") == task.url:
                entry["summary"] = summary
                entry["genres"] = genres
                return entry

        index_width = max(2, len(str(len(items) + 1)))
        entry = {
            "Anime_Index": str(len(items) + 1).zfill(index_width),
            "anime": task.title,
            "source_url": task.url,
            "summary": summary,
            "genres": genres,
            "poster_url": None,
            "total_episodes": task.total_episodes,
            "episodes": [],
        }
        items.append(entry)
        return entry

    def save_payload(self) -> None:
        self.payload["total_anime"] = len(self.payload.get("items", []))
        write_payload(self.payload)

    def load_tasks_from_payload(self) -> None:
        for item in self.payload.get("items", []):
            episodes = item.get("episodes", [])
            total_episodes = item.get("total_episodes")
            task = AnimeTask(
                url=item.get("source_url", ""),
                title=item.get("anime", ""),
                status="Done",
                total_episodes=total_episodes or len(episodes),
                saved_episodes=len(episodes),
                last_updated="—",
            )
            self.tasks.append(task)
        self.needs_refresh = True

    def refresh_tree(self) -> None:
        if not self.needs_refresh:
            return
        for widget in self.rows_frame.winfo_children():
            widget.destroy()

        for idx, task in enumerate(self.tasks):
            row = tk.Frame(self.rows_frame, bg="#0b0f14")
            row.grid(row=idx, column=0, sticky="ew", pady=2)
            row.columnconfigure(1, weight=1)

            var = tk.BooleanVar(value=task.selected)

            def toggle(t=task, v=var) -> None:
                t.selected = v.get()

            chk = ttk.Checkbutton(row, variable=var, command=toggle)
            chk.grid(row=0, column=0, padx=(4, 6))

            tk.Label(
                row,
                text=f"{task.title or task.url}",
                anchor="w",
                bg="#0b0f14",
                fg="#e8f1ff",
            ).grid(row=0, column=1, sticky="w")
            tk.Label(
                row,
                text=f"{task.status}",
                width=12,
                anchor="w",
                bg="#0b0f14",
                fg="#93c5fd",
            ).grid(row=0, column=2, padx=(10, 0))
            tk.Label(
                row,
                text=f"{task.saved_episodes}/{task.total_episodes}",
                width=12,
                anchor="center",
                bg="#0b0f14",
                fg="#93c5fd",
            ).grid(row=0, column=3)
            tk.Label(
                row,
                text=task.last_updated,
                width=14,
                anchor="center",
                bg="#0b0f14",
                fg="#93c5fd",
            ).grid(row=0, column=4)

        queued = sum(1 for t in self.tasks if t.status == "Queued")
        done = sum(1 for t in self.tasks if t.status == "Done")
        self.status_label.config(text=f"Queued: {queued} | Done: {done}")
        self.saved_label.config(text=f"Saved Anime: {len(self.payload.get('items', []))}")
        self.needs_refresh = False

    def _on_mousewheel(self, event: tk.Event) -> None:
        if event.delta:
            self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        else:
            if event.num == 4:
                self.canvas.yview_scroll(-1, "units")
            elif event.num == 5:
                self.canvas.yview_scroll(1, "units")

    def _bind_mousewheel(self) -> None:
        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)
        self.canvas.bind_all("<Button-4>", self._on_mousewheel)
        self.canvas.bind_all("<Button-5>", self._on_mousewheel)

    def _unbind_mousewheel(self) -> None:
        self.canvas.unbind_all("<MouseWheel>")
        self.canvas.unbind_all("<Button-4>")
        self.canvas.unbind_all("<Button-5>")

    def append_log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log.configure(state="normal")
        self.log.insert("end", f"[{timestamp}] {message}\n")
        self.log.see("end")
        self.log.configure(state="disabled")
        LOG_FILE.write_text(self.log.get("1.0", "end"), encoding="utf-8")

    def process_events(self) -> None:
        while True:
            try:
                event = self.event_queue.get_nowait()
            except queue.Empty:
                break

            event_type = event.get("type")
            task = event.get("task")

            if event_type == "task_start" and task:
                self.append_log(f"Scraping {task.url}.")
                self.needs_refresh = True
            elif event_type == "task_failed" and task:
                self.append_log(f"Failed to fetch {task.url}.")
                self.needs_refresh = True
            elif event_type == "task_skipped" and task:
                self.append_log(f"Skipped {task.url} (no episodes or OVA/Movie).")
                self.needs_refresh = True
            elif event_type == "episode_saved" and task:
                self.append_log(f"Saved episode for {task.title or task.url}.")
                self.needs_refresh = True
            elif event_type == "episode_failed" and task:
                episode = event.get("episode")
                self.append_log(f"Episode {episode} failed after retries for {task.title or task.url}.")
            elif event_type == "episode_skipped" and task:
                episode = event.get("episode")
                reason = event.get("reason", "Unknown")
                self.append_log(f"Episode {episode} skipped for {task.title or task.url}: {reason}.")
            elif event_type == "task_done" and task:
                self.append_log(f"Finished {task.title or task.url}.")
                self.needs_refresh = True
            elif event_type == "finished":
                self.badge.config(text="DONE", background="#22c55e")
                self.append_log("Manual scraping complete. M3U generated.")
                self.run_button.state(["!disabled"])
                self.pause_button.state(["disabled"])
                self.pause_button.config(text="Pause")
                self.stop_button.state(["disabled"])
                self.needs_refresh = True

        self.refresh_tree()
        self.root.after(200, self.process_events)


if __name__ == "__main__":
    root = tk.Tk()
    app = ManualScraperApp(root)
    root.mainloop()
