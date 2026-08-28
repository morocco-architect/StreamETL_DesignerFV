import sys

file_path = "c:\\projetBigdata\\frontend\\src\\index.css"

with open(file_path, "r", encoding="utf-8") as f:
    css = f.read()

# 1. Global replacements
css = css.replace("rgba(255, 255, 255, 0.03)", "var(--bg-glass)")
css = css.replace("rgba(255, 255, 255, 0.05)", "var(--bg-glass)")
css = css.replace("rgba(255, 255, 255, 0.08)", "var(--bg-glass-strong)")
css = css.replace("rgba(255, 255, 255, 0.1)", "var(--border-glass-strong)")
css = css.replace("rgba(255, 255, 255, 0.15)", "var(--border-glass-strong)")
css = css.replace("rgba(255, 255, 255, 0.2)", "var(--border-glass-strong)")
css = css.replace("rgba(255, 255, 255, 0.25)", "var(--border-glass-strong)")
css = css.replace("rgba(0, 0, 0, 0.2)", "var(--bg-code)")
css = css.replace("background: rgba(0, 0, 0, 0.3)", "background: var(--bg-code)")
css = css.replace("rgba(0, 0, 0, 0.3)", "var(--bg-code)") # In case I missed some
css = css.replace("#a5b4fc", "var(--text-code)")
css = css.replace("box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3)", "box-shadow: var(--shadow-md)")
css = css.replace("box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4)", "box-shadow: var(--shadow-md)")
css = css.replace("box-shadow: 0 10px 40px rgba(0, 0, 0, 0.4)", "box-shadow: var(--shadow-lg)")
css = css.replace("box-shadow: 0 10px 40px rgba(0, 0, 0, 0.5)", "box-shadow: var(--shadow-lg)")
css = css.replace("box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1)", "box-shadow: var(--shadow-sm)")

# Fix original variables that were replaced
css = css.replace("--border-glass: var(--bg-glass-strong);", "--border-glass: rgba(255, 255, 255, 0.08);")
css = css.replace("--bg-code: var(--bg-code);", "--bg-code: rgba(0, 0, 0, 0.3);") # Just in case

# 2. Add new variables to .dark
dark_vars = """  --text-muted: #888780;
  
  --bg-code: rgba(0, 0, 0, 0.3);
  --text-code: #a5b4fc;
  --shadow-sm: 0 2px 4px rgba(0, 0, 0, 0.1);
  --shadow-md: 0 8px 32px rgba(0, 0, 0, 0.4);
  --shadow-lg: 0 10px 40px rgba(0, 0, 0, 0.5);
  --bg-glass-strong: rgba(255, 255, 255, 0.08);
  --border-glass-strong: rgba(255, 255, 255, 0.2);"""
css = css.replace("  --text-muted: #888780;", dark_vars)

# 3. Add new variables to .light
light_vars = """  --text-muted: #8A8882;
  
  --bg-code: rgba(0, 0, 0, 0.05);
  --text-code: #4338ca;
  --shadow-sm: 0 2px 4px rgba(0, 0, 0, 0.05);
  --shadow-md: 0 8px 32px rgba(0, 0, 0, 0.1);
  --shadow-lg: 0 10px 40px rgba(0, 0, 0, 0.15);
  --bg-glass-strong: rgba(0, 0, 0, 0.06);
  --border-glass-strong: rgba(0, 0, 0, 0.15);"""
css = css.replace("  --text-muted: #8A8882;", light_vars)

with open(file_path, "w", encoding="utf-8") as f:
    f.write(css)

print("Replaced CSS.")
