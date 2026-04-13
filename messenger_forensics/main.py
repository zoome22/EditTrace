"""
Messenger Forensics Analyzer
메신저 포렌식 분석 도구 - GUI 메인 진입점
"""

import tkinter as tk
from ui.app import MessengerForensicsApp


def main():
    root = tk.Tk()
    app = MessengerForensicsApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
