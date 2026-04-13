"""
ui/app.py - 메신저 포렌식 분석 도구 메인 GUI
"""

import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

# 프로젝트 루트를 sys.path에 추가 (패키지 임포트용)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analyzers.registry import run_analysis

# ──────────────────────────────────────────────────────────────────────────────
# 색상 팔레트 & 상수
# ──────────────────────────────────────────────────────────────────────────────
C = {
    "bg":           "#0D1117",   # 최외곽 배경
    "panel":        "#161B22",   # 패널 배경
    "border":       "#30363D",   # 테두리
    "accent":       "#58A6FF",   # 포인트 파란색
    "accent_dim":   "#1F6FEB",   # 어두운 포인트
    "success":      "#3FB950",   # 성공 초록
    "warn":         "#D29922",   # 경고 노랑
    "error":        "#F85149",   # 에러 빨강
    "text":         "#E6EDF3",   # 기본 텍스트
    "text_dim":     "#8B949E",   # 흐린 텍스트
    "row_even":     "#161B22",
    "row_odd":      "#1C2128",
    "row_sel":      "#1F6FEB",
    "heading":      "#0D1117",
}

MESSENGERS = [
    "KakaoTalk",
    "Discord",
    "Telegram",
    "Facebook Messenger",
    "WhatsApp",
    "Instagram",
    "Jandi",
]

PLATFORMS = ["Android", "iOS"]

FONT_MONO  = ("Consolas", 9)
FONT_TABLE = ("Malgun Gothic", 9)  # 테이블 한글 볼드 방지용
FONT_LABEL = ("Segoe UI", 9)
FONT_TITLE = ("Segoe UI Semibold", 10)
FONT_HEAD  = ("Segoe UI Semibold", 13)


class MessengerForensicsApp:
    """메신저 포렌식 분석기 메인 애플리케이션."""

    def __init__(self, root: tk.Tk):
        self.root = root
        self._progress_running = False
        self._setup_window()
        self._apply_theme()
        self._build_ui()

    # ──────────────────────────────────────────────────────────────────────────
    # 초기화
    # ──────────────────────────────────────────────────────────────────────────

    def _setup_window(self):
        self.root.title("EditTrace")
        self.root.geometry("1200x820")
        self.root.minsize(900, 640)
        self.root.configure(bg=C["bg"])
        self.root.resizable(True, True)

        # 아이콘 (없으면 무시)
        try:
            self.root.iconbitmap("icon.ico")
        except Exception:
            pass

    def _apply_theme(self):
        """ttk 스타일을 다크 포렌식 테마로 설정합니다."""
        style = ttk.Style(self.root)
        style.theme_use("clam")

        # Treeview (결과 테이블)
        style.configure(
            "Forensic.Treeview",
            background=C["row_even"],
            foreground=C["text"],
            fieldbackground=C["row_even"],
            borderwidth=0,
            rowheight=24,
            font=FONT_TABLE,
        )
        style.configure(
            "Forensic.Treeview.Heading",
            background=C["heading"],
            foreground=C["accent"],
            font=FONT_TITLE,
            borderwidth=1,
            relief="flat",
        )
        style.map(
            "Forensic.Treeview",
            background=[("selected", C["row_sel"])],
            foreground=[("selected", "#FFFFFF")],
        )

        # Progressbar
        style.configure(
            "Forensic.Horizontal.TProgressbar",
            troughcolor=C["border"],
            background=C["accent"],
            thickness=4,
        )

        # Scrollbar
        style.configure(
            "Forensic.Vertical.TScrollbar",
            background=C["border"],
            troughcolor=C["panel"],
            arrowcolor=C["text_dim"],
        )
        style.configure(
            "Forensic.Horizontal.TScrollbar",
            background=C["border"],
            troughcolor=C["panel"],
            arrowcolor=C["text_dim"],
        )

        # Notebook
        style.configure(
            "Forensic.TNotebook",
            background=C["bg"],
            borderwidth=0,
        )
        style.configure(
            "Forensic.TNotebook.Tab",
            background=C["panel"],
            foreground=C["text_dim"],
            padding=[12, 5],
            font=FONT_LABEL,
        )
        style.map(
            "Forensic.TNotebook.Tab",
            background=[("selected", C["bg"])],
            foreground=[("selected", C["accent"])],
        )

    # ──────────────────────────────────────────────────────────────────────────
    # UI 빌드
    # ──────────────────────────────────────────────────────────────────────────

    def _build_ui(self):
        self._build_header()
        self._build_body()
        self._build_statusbar()

    def _build_header(self):
        hdr = tk.Frame(self.root, bg=C["panel"], height=56)
        hdr.pack(fill="x", side="top")
        hdr.pack_propagate(False)

        # 좌측 타이틀
        tk.Label(
            hdr,
            text="⬡  MESSENGER FORENSICS ANALYZER",
            font=("Consolas", 13, "bold"),
            bg=C["panel"],
            fg=C["accent"],
        ).pack(side="left", padx=20, pady=14)

        # 우측 버전
        tk.Label(
            hdr,
            text="v0.1.0  |  forensic edition",
            font=FONT_MONO,
            bg=C["panel"],
            fg=C["text_dim"],
        ).pack(side="right", padx=20)

        # 구분선
        tk.Frame(self.root, bg=C["border"], height=1).pack(fill="x")

    def _build_body(self):
        body = tk.Frame(self.root, bg=C["bg"])
        body.pack(fill="both", expand=True, padx=0, pady=0)

        # 좌측 컨트롤 패널
        left = tk.Frame(body, bg=C["panel"], width=300)
        left.pack(side="left", fill="y")
        left.pack_propagate(False)
        self._build_control_panel(left)

        # 구분선
        tk.Frame(body, bg=C["border"], width=1).pack(side="left", fill="y")

        # 우측 결과 패널
        right = tk.Frame(body, bg=C["bg"])
        right.pack(side="left", fill="both", expand=True)
        self._build_result_panel(right)

    def _build_control_panel(self, parent):
        """좌측 컨트롤 패널: 파일 선택 / 플랫폼 / 메신저 / 분석 버튼."""

        def section_label(text):
            tk.Label(
                parent,
                text=text,
                font=("Consolas", 8),
                bg=C["panel"],
                fg=C["text_dim"],
                anchor="w",
            ).pack(fill="x", padx=20, pady=(18, 4))
            tk.Frame(parent, bg=C["border"], height=1).pack(fill="x", padx=20)

        # ── 파일 선택 ──────────────────────────────────────────────────────
        section_label("[ TARGET ]")

        path_frame = tk.Frame(parent, bg=C["panel"])
        path_frame.pack(fill="x", padx=20, pady=(8, 0))

        self.path_var = tk.StringVar(value="경로를 선택하세요")
        path_entry = tk.Entry(
            path_frame,
            textvariable=self.path_var,
            font=FONT_MONO,
            bg=C["bg"],
            fg=C["text"],
            insertbackground=C["accent"],
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightcolor=C["accent"],
            highlightbackground=C["border"],
        )
        path_entry.pack(fill="x", ipady=6)

        btn_frame = tk.Frame(parent, bg=C["panel"])
        btn_frame.pack(fill="x", padx=20, pady=(6, 0))

        self._make_btn(btn_frame, "📁  파일 선택", self._pick_file).pack(
            side="left", fill="x", expand=True, padx=(0, 4)
        )
        self._make_btn(btn_frame, "📂  폴더 선택", self._pick_folder).pack(
            side="left", fill="x", expand=True, padx=(4, 0)
        )

        # ── 플랫폼 ─────────────────────────────────────────────────────────
        section_label("[ PLATFORM ]")

        self.platform_var = tk.StringVar(value="Android")
        plat_row = tk.Frame(parent, bg=C["panel"])
        plat_row.pack(fill="x", padx=20, pady=(8, 0))
        for p in PLATFORMS:
            self._make_radio(plat_row, p, self.platform_var, p).pack(
                side="left", padx=(0, 16)
            )

        # ── 메신저 ─────────────────────────────────────────────────────────
        section_label("[ MESSENGER ]")

        self.messenger_var = tk.StringVar(value=MESSENGERS[0])
        msg_frame = tk.Frame(parent, bg=C["panel"])
        msg_frame.pack(fill="x", padx=20, pady=(8, 0))
        for m in MESSENGERS:
            self._make_radio(msg_frame, m, self.messenger_var, m).pack(
                anchor="w", pady=1
            )

        # ── 분석 버튼 ──────────────────────────────────────────────────────
        tk.Frame(parent, bg=C["border"], height=1).pack(fill="x", padx=20, pady=(24, 0))

        self.analyze_btn = tk.Button(
            parent,
            text="▶  분 석 시 작",
            font=("Consolas", 11, "bold"),
            bg=C["accent_dim"],
            fg="#FFFFFF",
            activebackground=C["accent"],
            activeforeground="#FFFFFF",
            relief="flat",
            bd=0,
            cursor="hand2",
            pady=12,
            command=self._start_analysis,
        )
        self.analyze_btn.pack(fill="x", padx=20, pady=(12, 0))

        # ── 진행 바 ───────────────────────────────────────────────────────
        # clam 테마 기본 Progressbar에 색상만 덮어씀
        _ps = ttk.Style()
        _ps.configure("Busy.Horizontal.TProgressbar",
                       troughcolor=C["border"],
                       background=C["accent"],
                       thickness=6)
        self.progress = ttk.Progressbar(
            parent,
            style="Busy.Horizontal.TProgressbar",
            mode="determinate",
            maximum=100,
        )
        self.progress.pack(fill="x", padx=20, pady=(10, 0))

    def _build_result_panel(self, parent):
        """우측 결과 패널: 요약 카드 + 탭 테이블."""

        # 상단 요약 바
        self.summary_frame = tk.Frame(parent, bg=C["bg"])
        self.summary_frame.pack(fill="x", padx=16, pady=(14, 0))

        self._summary_placeholder()

        tk.Frame(parent, bg=C["border"], height=1).pack(fill="x", padx=16, pady=(12, 0))

        # 탭 Notebook
        self.notebook = ttk.Notebook(parent, style="Forensic.TNotebook")
        self.notebook.pack(fill="both", expand=True, padx=16, pady=(8, 0))

        self._result_placeholder()

        # 에러 로그 영역
        tk.Frame(parent, bg=C["border"], height=1).pack(fill="x", padx=16, pady=(8, 0))
        log_hdr = tk.Frame(parent, bg=C["bg"])
        log_hdr.pack(fill="x", padx=16, pady=(4, 2))
        tk.Label(
            log_hdr,
            text="LOG",
            font=("Consolas", 8),
            bg=C["bg"],
            fg=C["text_dim"],
        ).pack(side="left")

        log_frame = tk.Frame(parent, bg=C["bg"])
        log_frame.pack(fill="x", padx=16, pady=(0, 12))

        self.log_text = tk.Text(
            log_frame,
            height=4,
            font=FONT_MONO,
            bg=C["heading"],
            fg=C["text_dim"],
            relief="flat",
            bd=0,
            state="disabled",
            wrap="word",
            highlightthickness=1,
            highlightbackground=C["border"],
        )
        self.log_text.pack(fill="x")

    def _build_statusbar(self):
        tk.Frame(self.root, bg=C["border"], height=1).pack(fill="x")
        bar = tk.Frame(self.root, bg=C["panel"], height=26)
        bar.pack(fill="x", side="bottom")
        bar.pack_propagate(False)

        self.status_var = tk.StringVar(value="대기 중")
        tk.Label(
            bar,
            textvariable=self.status_var,
            font=FONT_MONO,
            bg=C["panel"],
            fg=C["text_dim"],
            anchor="w",
        ).pack(side="left", padx=12, pady=4)

    # ──────────────────────────────────────────────────────────────────────────
    # 헬퍼 위젯 팩토리
    # ──────────────────────────────────────────────────────────────────────────

    def _make_btn(self, parent, text, cmd):
        return tk.Button(
            parent,
            text=text,
            font=FONT_LABEL,
            bg=C["bg"],
            fg=C["text"],
            activebackground=C["border"],
            activeforeground=C["text"],
            relief="flat",
            bd=0,
            cursor="hand2",
            pady=6,
            command=cmd,
            highlightthickness=1,
            highlightbackground=C["border"],
            highlightcolor=C["accent"],
        )

    def _make_radio(self, parent, text, variable, value):
        return tk.Radiobutton(
            parent,
            text=text,
            variable=variable,
            value=value,
            font=FONT_LABEL,
            bg=C["panel"],
            fg=C["text"],
            selectcolor=C["bg"],
            activebackground=C["panel"],
            activeforeground=C["accent"],
            indicatoron=True,
            relief="flat",
            cursor="hand2",
        )

    def _summary_placeholder(self):
        for w in self.summary_frame.winfo_children():
            w.destroy()
        tk.Label(
            self.summary_frame,
            text="분석을 실행하면 요약 정보가 여기에 표시됩니다.",
            font=FONT_LABEL,
            bg=C["bg"],
            fg=C["text_dim"],
        ).pack(anchor="w")

    def _result_placeholder(self):
        for tab in self.notebook.tabs():
            self.notebook.forget(tab)

        ph = tk.Frame(self.notebook, bg=C["bg"])
        self.notebook.add(ph, text="결과")

        tk.Label(
            ph,
            text="\n\n\n⬡\n\n분석 결과가 여기에 표시됩니다.\n대상 파일/폴더를 선택한 뒤 '분석 시작'을 누르세요.",
            font=("Consolas", 11),
            bg=C["bg"],
            fg=C["text_dim"],
            justify="center",
        ).pack(expand=True)

    # ──────────────────────────────────────────────────────────────────────────
    # 이벤트 핸들러
    # ──────────────────────────────────────────────────────────────────────────

    def _pick_file(self):
        path = filedialog.askopenfilename(
            title="분석할 데이터베이스 파일 선택",
            filetypes=[
                ("DB files", "*.db *.sqlite *.sqlite3"),
                ("All files", "*.*"),
            ],
        )
        if path:
            self.path_var.set(path)

    def _pick_folder(self):
        path = filedialog.askdirectory(title="분석할 폴더 선택")
        if path:
            self.path_var.set(path)

    def _start_analysis(self):
        raw_path = self.path_var.get().strip()
        if not raw_path or raw_path == "경로를 선택하세요":
            messagebox.showwarning("경고", "분석할 파일 또는 폴더를 선택하세요.")
            return

        path = Path(raw_path)
        messenger = self.messenger_var.get()
        platform  = self.platform_var.get()

        self._set_busy(True)
        self._log_clear()
        self._log(f"분석 시작: {messenger} / {platform} — {path}")
        self.status_var.set(f"분석 중: {messenger} [{platform}] ...")

        thread = threading.Thread(
            target=self._analysis_worker,
            args=(path, messenger, platform),
            daemon=True,
        )
        thread.start()

    def _analysis_worker(self, path: Path, messenger: str, platform: str):
        """백그라운드 스레드에서 분석을 실행합니다."""
        try:
            result = run_analysis(path, messenger, platform)
        except Exception as exc:
            self.root.after(0, self._on_analysis_error, str(exc))
            return
        self.root.after(0, self._on_analysis_done, result, messenger, platform)

    # ──────────────────────────────────────────────────────────────────────────
    # 결과 렌더링
    # ──────────────────────────────────────────────────────────────────────────

    def _on_analysis_done(self, result, messenger: str, platform: str):
        self._set_busy(False)

        if not result.success:
            self.status_var.set(f"완료 (경고 있음) — {messenger} [{platform}]")
        else:
            self.status_var.set(f"분석 완료 — {messenger} [{platform}]")

        # 에러 로그
        for err in result.errors:
            self._log(f"[WARN] {err}", color=C["warn"])

        # 요약 카드 렌더링
        self._render_summary(result.summary, result.success)

        # 테이블 탭 렌더링
        self._render_tables(result.tables)

    def _on_analysis_error(self, error_msg: str):
        self._set_busy(False)
        self.status_var.set("분석 실패")
        self._log(f"[ERROR] {error_msg}", color=C["error"])
        messagebox.showerror("분석 오류", f"분석 중 오류가 발생했습니다:\n\n{error_msg}")

    def _render_summary(self, summary: dict, success: bool):
        for w in self.summary_frame.winfo_children():
            w.destroy()

        if not summary:
            self._summary_placeholder()
            return

        for key, val in summary.items():
            card = tk.Frame(
                self.summary_frame,
                bg=C["panel"],
                highlightthickness=1,
                highlightbackground=C["border"],
            )
            card.pack(side="left", padx=(0, 8), ipadx=12, ipady=6)

            tk.Label(
                card,
                text=key,
                font=("Consolas", 7),
                bg=C["panel"],
                fg=C["text_dim"],
            ).pack(anchor="w", padx=8, pady=(4, 0))

            if key == "수정 이력 메시지" and val != "0":
                color = "#F0883E"
            elif success:
                color = C["success"]
            else:
                color = C["warn"]
            tk.Label(
                card,
                text=val,
                font=("Consolas", 10, "bold"),
                bg=C["panel"],
                fg=color,
            ).pack(anchor="w", padx=8, pady=(2, 4))

    def _render_tables(self, tables: list):
        for tab in self.notebook.tabs():
            self.notebook.forget(tab)

        if not tables:
            self._result_placeholder()
            return

        for tbl in tables:
            self._add_table_tab(
                tbl["title"],
                tbl["columns"],
                tbl["rows"],
                tbl.get("highlight_rows", set()),
                tbl.get("sub_rows", {}),
            )

    def _add_table_tab(
        self,
        title: str,
        columns: list[str],
        rows: list[list[str]],
        highlight_rows: set = None,
        sub_rows: dict = None,
    ):
        highlight_rows = highlight_rows or set()
        sub_rows       = sub_rows or {}

        frame = tk.Frame(self.notebook, bg=C["bg"])
        hl_count = len(highlight_rows)
        tab_text = f"  {title}  " if hl_count == 0 else f"  {title}  ●{hl_count}  "
        self.notebook.add(frame, text=tab_text)

        # 스크롤바
        vsb = ttk.Scrollbar(frame, orient="vertical",   style="Forensic.Vertical.TScrollbar")
        hsb = ttk.Scrollbar(frame, orient="horizontal", style="Forensic.Horizontal.TScrollbar")

        # 서브행이 있을 때만 tree 모드 + 인스턴스 전용 스타일로 indent 최소화
        if sub_rows:
            _st = ttk.Style()
            _sname = f"FT{id(frame)}.Treeview"
            _st.configure(_sname,
                background=C["row_even"], foreground=C["text"],
                fieldbackground=C["row_even"], borderwidth=0,
                rowheight=24, font=FONT_TABLE, indent=14,
            )
            _st.configure(f"{_sname}.Heading",
                background=C["heading"], foreground=C["accent"],
                font=FONT_TITLE, borderwidth=1, relief="flat",
            )
            _st.map(_sname,
                background=[("selected", C["row_sel"])],
                foreground=[("selected", "#FFFFFF")],
            )
            _tree_style = _sname
            _tree_show  = "tree headings"
        else:
            _tree_style = "Forensic.Treeview"
            _tree_show  = "headings"

        tree = ttk.Treeview(
            frame,
            columns=columns,
            show=_tree_show,
            style=_tree_style,
            yscrollcommand=vsb.set,
            xscrollcommand=hsb.set,
        )
        vsb.configure(command=tree.yview)
        hsb.configure(command=tree.xview)

        # #0 컬럼: 토글 아이콘 너비(indent=14)에 딱 맞게, 나머지는 숨김
        if sub_rows:
            tree.column("#0", width=14, minwidth=14, stretch=False)
        else:
            tree.column("#0", width=0,  minwidth=0,  stretch=False)

        # 컬럼 너비
        COL_WIDTHS = {
            "앱 내 표시 메시지 / 최종 수정 메시지": (340, 160),
            "앱 내 표시 메시지":                    (300, 160),
            "수정 당시 내용":                        (300, 160),
            "전송/수정 시각":                        (165, 165),  # YYYY-MM-DD HH:MM:SS 고정
            "전송 시각":                             (165, 165),
            "수정 시각":                             (165, 165),
            "DB파일":                                (120,  60),
            "DB 파일":                               (120,  60),
            "채널 ID":                               (160,  80),
            "메시지 ID":                             (160,  80),
            "UserID":                                ( 90,  60),
            "작성자":                                ( 90,  60),
            "ID":                                    ( 90,  90),
            "수정 횟수":                             ( 55,  55),
        }
        for col in columns:
            w, mw = COL_WIDTHS.get(col, (120, 60))
            tree.heading(col, text=col, anchor="w")
            tree.column(col, width=w, minwidth=mw, anchor="w")

        # ── 태그 ──────────────────────────────────────────────────────────
        # 부모 행
        tree.tag_configure("even",      background=C["row_even"], foreground=C["text"])
        tree.tag_configure("odd",       background=C["row_odd"],  foreground=C["text"])
        tree.tag_configure("highlight", background="#F0883E",     foreground="#0D1117",
                           font=("Consolas", 9))
        # 서브 행 (수정 이력)
        tree.tag_configure("sub",       background="#0D2137",     foreground="#A8D8FF",
                           font=("Consolas", 8))
        tree.tag_configure("sub_first", background="#0D2137",     foreground="#79C0FF",
                           font=("Consolas", 8))

        # ── 행 삽입 ───────────────────────────────────────────────────────
        for i, row in enumerate(rows):
            if i in highlight_rows:
                p_tag = "highlight"
            else:
                p_tag = "even" if i % 2 == 0 else "odd"

            pid = tree.insert("", "end",
                              values=[str(v) for v in row],
                              tags=(p_tag,),
                              open=True)

            # 서브 행: 진짜 자식으로 삽입
            if i in sub_rows:
                for j, child in enumerate(sub_rows[i]):
                    c_tag = "sub_first" if j == 0 else "sub"
                    tree.insert(pid, "end",
                                values=[str(v) for v in child],
                                tags=(c_tag,))

        # 범례 + 행 수
        footer = tk.Frame(frame, bg=C["bg"])
        footer.pack(side="bottom", fill="x", pady=(4, 6))
        tk.Label(footer, text=f"  총 {len(rows)}행", font=FONT_MONO,
                 bg=C["bg"], fg=C["text_dim"]).pack(side="left", padx=4)
        if hl_count:
            tk.Label(footer, text=f"  ● 수정 이력 있음: {hl_count}건",
                     font=("Consolas", 9, "bold"),
                     bg=C["bg"], fg="#F0883E").pack(side="left", padx=8)


        # 레이아웃
        vsb.pack(side="right", fill="y")
        hsb.pack(side="bottom", fill="x")
        tree.pack(side="left", fill="both", expand=True)

    # ──────────────────────────────────────────────────────────────────────────
    # 유틸리티
    # ──────────────────────────────────────────────────────────────────────────

    def _set_busy(self, busy: bool):
        if busy:
            self.analyze_btn.configure(state="disabled", text="⏳  분석 중...")
            self._progress_running = True
            self._progress_dir = 1
            self._progress_val = 0
            self._progress_tick()
        else:
            self._progress_running = False
            self.progress["value"] = 100  # 완료 시 꽉 채움
            self.analyze_btn.configure(state="normal", text="▶  분 석 시 작")

    def _progress_tick(self):
        """determinate 모드로 0↔100 왕복 애니메이션."""
        if not self._progress_running:
            return
        self._progress_val += self._progress_dir * 3
        if self._progress_val >= 100:
            self._progress_val = 100
            self._progress_dir = -1
        elif self._progress_val <= 0:
            self._progress_val = 0
            self._progress_dir = 1
        self.progress["value"] = self._progress_val
        self.root.after(30, self._progress_tick)

    def _log(self, msg: str, color: str = None):
        self.log_text.configure(state="normal")
        tag = f"color_{color}" if color else None
        if color:
            self.log_text.tag_configure(tag, foreground=color)
        self.log_text.insert("end", msg + "\n", tag or ())
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _log_clear(self):
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.configure(state="disabled")
