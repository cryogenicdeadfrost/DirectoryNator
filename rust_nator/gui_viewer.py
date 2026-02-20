import json
import os
import tkinter as tk
from tkinter import filedialog, ttk


class App:
    def __init__(self, root):
        self.root = root
        self.root.title("DirectoryNator Rust Result Viewer")
        self.data = None

        top = ttk.Frame(root)
        top.pack(fill="x", padx=10, pady=8)
        ttk.Button(top, text="Open JSON", command=self.open_file).pack(side="left")
        self.lbl = ttk.Label(top, text="No file loaded")
        self.lbl.pack(side="left", padx=10)

        self.tbl = ttk.Treeview(root, columns=("workers", "ms", "files", "fps", "depth", "score", "den", "err"), show="headings", height=10)
        for c in self.tbl["columns"]:
            self.tbl.heading(c, text=c)
            self.tbl.column(c, width=90, anchor="center")
        self.tbl.pack(fill="both", expand=True, padx=10, pady=6)

        self.cv = tk.Canvas(root, height=260, bg="#111")
        self.cv.pack(fill="both", expand=True, padx=10, pady=6)

        self.meta = tk.Text(root, height=8)
        self.meta.pack(fill="both", expand=False, padx=10, pady=6)

    def open_file(self):
        p = filedialog.askopenfilename(
            title="Open Rust JSON result",
            initialdir=os.path.join(os.getcwd(), "out"),
            filetypes=[("JSON", "*.json")],
        )
        if not p:
            return
        with open(p, "r", encoding="utf-8") as f:
            self.data = json.load(f)
        self.lbl.configure(text=p)
        self.render()

    def render(self):
        for i in self.tbl.get_children():
            self.tbl.delete(i)
        self.cv.delete("all")
        self.meta.delete("1.0", "end")

        stats = self.data.get("stats", []) if isinstance(self.data, dict) else []
        mode = self.data.get("mode", "unknown") if isinstance(self.data, dict) else "unknown"
        hw = self.data.get("hw", {}) if isinstance(self.data, dict) else {}
        root = self.data.get("root", "") if isinstance(self.data, dict) else ""

        for s in stats:
            self.tbl.insert("", "end", values=(
                s.get("wk", ""),
                s.get("ms", ""),
                s.get("files", ""),
                s.get("fps", ""),
                s.get("deep", ""),
                s.get("score", ""),
                s.get("den", ""),
                s.get("err", ""),
            ))

        self.draw_graph(stats)
        self.meta.insert("end", f"mode: {mode}\nroot: {root}\n")
        self.meta.insert("end", f"hardware: {hw}\n")

    def draw_graph(self, stats):
        if not stats:
            self.cv.create_text(200, 120, text="No stats in file", fill="white", font=("Arial", 14))
            return
        w = max(self.cv.winfo_width(), 500)
        h = max(self.cv.winfo_height(), 260)
        pad = 30
        bw = max(20, (w - 2 * pad) // max(1, len(stats) * 2))

        max_ms = max(float(s.get("ms", 0) or 0) for s in stats) or 1
        max_sc = max(float(s.get("score", 0) or 0) for s in stats) or 1

        x = pad
        for s in stats:
            ms = float(s.get("ms", 0) or 0)
            sc = float(s.get("score", 0) or 0)
            wk = s.get("wk", "")

            mh = (ms / max_ms) * (h - 2 * pad)
            sh = (sc / max_sc) * (h - 2 * pad)

            self.cv.create_rectangle(x, h - pad - mh, x + bw, h - pad, fill="#4fc3f7", outline="")
            self.cv.create_rectangle(x + bw + 4, h - pad - sh, x + 2 * bw + 4, h - pad, fill="#81c784", outline="")
            self.cv.create_text(x + bw, h - 10, text=str(wk), fill="white", font=("Arial", 9))
            x += (2 * bw + 18)

        self.cv.create_text(90, 12, text="blue=ms  green=score", fill="white", font=("Arial", 10))


if __name__ == "__main__":
    r = tk.Tk()
    r.geometry("980x760")
    App(r)
    r.mainloop()
