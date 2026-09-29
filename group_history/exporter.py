"""《群史》编纂委员会 - 出版 Exporter

把全部词条编译成一本可长期保存的《群史》HTML（自带排版，浏览器打开即可打印为 PDF）。
"""

from __future__ import annotations

import html as _html
import json
from datetime import datetime
from pathlib import Path


def _esc(text: str) -> str:
    return _html.escape(str(text or ""))


def build_book_html(entries: list[dict], group_name: str = "") -> str:
    """entries: storage 的词条 dict 列表（按 serial 升序传入）。"""
    now = datetime.now()
    date_range = "—"
    if entries:
        date_range = f"{entries[0]['first_date']} 至 {entries[-1]['updated_at'][:10]}"
    total_revisions = sum(e.get("revision_count", 1) for e in entries)

    # 目录
    toc_items = "\n".join(
        f'<li><a href="#entry-{e["serial"]}">'
        f'<span class="toc-serial">第 {e["serial"]:04d} 号</span>{_esc(e["title"])}</a></li>'
        for e in entries
    )

    # 正文
    articles = []
    for e in entries:
        try:
            aliases = json.loads(e.get("aliases") or "[]")
        except Exception:
            aliases = []
        try:
            cats = json.loads(e.get("categories") or "[]")
        except Exception:
            cats = []
        alias_html = ""
        if aliases:
            alias_html = (
                '<div class="aliases">又稱：'
                + "、".join(_esc(a) for a in aliases)
                + "</div>"
            )
        cat_html = ""
        if cats:
            cat_html = '<div class="cats">' + " ".join(
                f'<span class="cat">{_esc(c)}</span>' for c in cats
            ) + "</div>"
        sig = int(e.get("significance") or 5)
        stars = "★" * max(1, min(5, round(sig / 2)))
        articles.append(f"""
<article id="entry-{e["serial"]}">
  <h2>{_esc(e["title"])}</h2>
  {alias_html}
  <div class="meta-line">
    <span>群史字第 {e["serial"]:04d} 号</span>
    <span>事发：{_esc(e["first_date"])}</span>
    <span>载入：{_esc(e["created_at"][:10])}</span>
    <span>修订 {e.get("revision_count", 1)} 次</span>
    <span>史评等级 {stars}</span>
  </div>
  <div class="content">{_esc(e["content"])}</div>
  {cat_html}
</article>""")

    # 分类索引
    cat_map: dict[str, list[int]] = {}
    for e in entries:
        try:
            cats = json.loads(e.get("categories") or "[]")
        except Exception:
            cats = []
        for c in cats or []:
            cat_map.setdefault(str(c), []).append(e["serial"])
    cat_index = ""
    if cat_map:
        rows = "".join(
            f'<tr><td class="cat-name">{_esc(c)}</td><td>{len(s)}</td>'
            f'<td>{"、".join(f"{x:04d}" for x in sorted(s)[:20])}</td></tr>'
            for c, s in sorted(cat_map.items(), key=lambda kv: -kv[1])
        )
        cat_index = f"""
<section class="backmatter">
  <h2>分类索引</h2>
  <table class="index-table">
    <tr><th>分类</th><th>词条数</th><th>编号（至多列前 20）</th></tr>
    {rows}
  </table>
</section>"""

    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>群史 · {group_name or "编纂委员会"}</title>
<style>
  @page {{ margin: 2cm; }}
  body {{
    font-family: "Noto Serif SC", "Source Han Serif SC", "SimSun", serif;
    max-width: 800px; margin: 0 auto; padding: 32px 24px;
    color: #1a1a1a; line-height: 1.9; background: #faf8f3;
  }}
  .cover {{ text-align: center; padding: 120px 0 80px; page-break-after: always; }}
  .cover h1 {{ font-size: 56px; letter-spacing: 24px; margin: 0 0 16px; text-indent: 24px; }}
  .cover .sub {{ font-size: 18px; color: #555; letter-spacing: 6px; }}
  .cover .meta {{ margin-top: 64px; color: #777; font-size: 14px; line-height: 2; }}
  .cover .seal {{
    display: inline-block; margin-top: 48px; padding: 14px 22px;
    border: 3px solid #a33; border-radius: 6px; color: #a33;
    font-size: 20px; letter-spacing: 8px; text-indent: 8px; transform: rotate(-6deg);
  }}
  nav {{ page-break-after: always; }}
  nav h2 {{ border-bottom: 2px solid #1a1a1a; padding-bottom: 8px; }}
  nav ol {{ padding-left: 24px; }}
  nav li {{ margin: 6px 0; }}
  .toc-serial {{ display: inline-block; min-width: 110px; color: #888; font-size: 13px; }}
  article {{ margin: 40px 0; padding: 24px 28px; background: #fff;
             border: 1px solid #d8d2c4; border-radius: 4px;
             box-shadow: 0 1px 3px rgba(0,0,0,.06); page-break-inside: avoid; }}
  article h2 {{ margin: 0 0 4px; font-size: 24px; border: none; padding: 0; }}
  .aliases {{ color: #666; font-size: 13px; margin-bottom: 8px; }}
  .meta-line {{ display: flex; flex-wrap: wrap; gap: 6px 18px; font-size: 12.5px; color: #777;
                border-top: 1px solid #eee; border-bottom: 1px solid #eee;
                padding: 8px 0; margin: 12px 0 16px; }}
  .content {{ font-size: 15.5px; text-align: justify; white-space: pre-wrap; }}
  .cats {{ margin-top: 14px; }}
  .cat {{ display: inline-block; background: #eee7d8; color: #6b5d3f; font-size: 12px;
          padding: 2px 10px; border-radius: 10px; margin-right: 6px; }}
  .backmatter {{ margin-top: 56px; page-break-before: always; }}
  .backmatter h2 {{ border-bottom: 2px solid #1a1a1a; padding-bottom: 8px; }}
  .index-table {{ width: 100%; border-collapse: collapse; font-size: 13.5px; }}
  .index-table th, .index-table td {{ border: 1px solid #ccc; padding: 6px 10px; text-align: left; }}
  .index-table th {{ background: #efe9dc; }}
  .cat-name {{ font-weight: bold; }}
  .colophon {{ margin-top: 64px; text-align: center; color: #888; font-size: 13px; line-height: 2.2; }}
</style>
</head>
<body>
  <div class="cover">
    <h1>群 史</h1>
    <div class="sub">{_esc(group_name or "某群")} 正史 · 第一卷</div>
    <div class="meta">
      记事起讫：{_esc(date_range)}<br>
      收录词条 {len(entries)} 篇 · 累计修订 {total_revisions} 次<br>
      纂修：群史编纂委员会<br>
      {now.strftime("%Y 年 %m 月 %d 日")} 出版
    </div>
    <div class="seal">编纂委员会之印</div>
  </div>
  <nav>
    <h2>目录</h2>
    <ol>{toc_items}</ol>
  </nav>
  {''.join(articles)}
  {cat_index}
  <div class="colophon">
    —— 全书终 ——<br>
    本史由群史编纂委员会逐日编纂，所引《群聊实录》均出自本群群友之口。<br>
    如有史实错漏，可向编委会申诉；编委会有权拒绝更正。
  </div>
</body>
</html>"""


def export_book(entries: list[dict], out_dir: Path, group_name: str = "") -> tuple[Path, Path | None]:
    """写出 HTML，若本机装有 weasyprint 则同时产出 PDF。返回 (html_path, pdf_path|None)。"""
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d")
    html_path = out_dir / f"群史_第一卷_{stamp}.html"
    html_path.write_text(build_book_html(entries, group_name), encoding="utf-8")
    pdf_path = None
    try:
        from weasyprint import HTML  # type: ignore

        pdf_path = out_dir / f"群史_第一卷_{stamp}.pdf"
        HTML(string=build_book_html(entries, group_name)).write_pdf(str(pdf_path))
    except Exception:
        pdf_path = None
    return html_path, pdf_path
