"""Copies the text of LSE's API and WebSocket documentation beside this script.

LSE publishes its documentation on its site as page modules of its JavaScript
bundle, which this reads as text and never runs: each string of a page, a
paragraph a line, its inline code joined into it; table rows and objects of
strings as cells joined by " | "; class names, element tags and import paths
left out, so that a redeploy that changes no text changes no line.

Usage: python3 crates/lse/upstream/fetch.py
Then `git diff crates/lse/upstream/` shows what LSE changed since the copy
committed last (see the crate's README, "LSE's documentation").
"""
import re
import sys
import urllib.request
from pathlib import Path

SITE = "https://londonstrategicedge.com"
# The copy of each page, by the name of its module in LSE's bundle.
PAGES = {
    "ApiDocumentation": "api-documentation.txt",
    "WebSocketDocumentation": "websocket-documentation.txt",
}


def get(url):
    request = urllib.request.Request(url, headers={"User-Agent": "lse-docs-copy"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.headers.get_content_type(), response.read().decode("utf-8")


def module(url):
    """The JavaScript at `url`, failing on the page LSE's site answers a
    file it does not have with."""
    kind, text = get(url)
    if kind != "text/javascript" and kind != "application/javascript":
        sys.exit(f"{url} answered {kind}, not a script: LSE's site has no such file")
    return text


def one(pattern, text, where):
    found = sorted(set(re.findall(pattern, text)))
    if len(found) != 1:
        sys.exit(f"{where}: {len(found)} matches of {pattern}, not one: {found}")
    return found[0]


# Tokens: a string, a punctuation mark, or a word.
STRING, MARK, WORD = "string", "mark", "word"
ESCAPES = {"n": "\n", "t": "\t", "r": "\r", "b": "\b", "f": "\f", "v": "\v", "0": "\0"}


def tokens(src):
    i, n = 0, len(src)
    while i < n:
        c = src[i]
        if c.isspace():
            i += 1
        elif c in "\"'`":
            value, i = string(src, i)
            yield STRING, value
        elif c.isalnum() or c in "_$":
            j = i
            while j < n and (src[j].isalnum() or src[j] in "_$"):
                j += 1
            yield WORD, src[i:j]
            i = j
        else:
            yield MARK, c
            i += 1


def string(src, i):
    """The string literal opening at `i`, its escapes read, a template's
    substitutions kept as `${...}` with their expression; and where it ends."""
    quote, out, i = src[i], [], i + 1
    while src[i] != quote:
        c = src[i]
        if c == "\\":
            e = src[i + 1]
            if e == "u" and src[i + 2] == "{":
                end = src.index("}", i)
                out.append(chr(int(src[i + 3 : end], 16)))
                i = end + 1
            elif e == "u":
                out.append(chr(int(src[i + 2 : i + 6], 16)))
                i += 6
            elif e == "x":
                out.append(chr(int(src[i + 2 : i + 4], 16)))
                i += 4
            elif e == "\n":
                i += 2
            else:
                out.append(ESCAPES.get(e, e))
                i += 2
        elif quote == "`" and src.startswith("${", i):
            depth, j = 1, i + 2
            while depth:
                depth += {"{": 1, "}": -1}.get(src[j], 0)
                j += 1
            out.append(src[i:j])
            i = j
        else:
            out.append(c)
            i += 1
    return "".join(out), i + 1


def tree(src):
    """The tokens of `src` nested by their brackets: each bracketed run a
    list opening with its bracket."""
    stack = [["("]]
    for kind, value in tokens(src):
        if kind == MARK and value in "([{":
            stack.append([value])
        elif kind == MARK and value in ")]}":
            group = stack.pop()
            if not stack or {"(": ")", "[": "]", "{": "}"}[group[0]] != value:
                sys.exit(f"unbalanced {value} in LSE's page module")
            stack[-1].append(group)
        else:
            stack[-1].append((kind, value))
    if len(stack) != 1:
        sys.exit("an unclosed bracket in LSE's page module")
    return stack[0]


def is_group(item, bracket=None):
    return isinstance(item, list) and (bracket is None or item[0] == bracket)


def is_string(item):
    return isinstance(item, tuple) and item[0] == STRING


def parts(group):
    """The elements of a bracketed run, split at its commas."""
    out, part = [], []
    for item in group[1:]:
        if item == (MARK, ","):
            out.append(part)
            part = []
        else:
            part.append(item)
    if part:
        out.append(part)
    return out


def jsx_props(part):
    """The props of an element made by `x.jsx(type, props)`, or none."""
    if len(part) == 4 and part[1] == (MARK, ".") and part[2] in ((WORD, "jsx"), (WORD, "jsxs")):
        args = parts(part[3])
        if len(args) >= 2 and len(args[1]) == 1 and is_group(args[1][0], "{"):
            return args[1][0]
    return None


def entries(obj):
    """The `key: value` entries of an object, each as its key and value."""
    for part in parts(obj):
        if len(part) >= 3 and part[1] == (MARK, ":") and isinstance(part[0], tuple):
            yield part[0][1], part[2:]
        else:
            yield None, part


class Text:
    def __init__(self, consts):
        self.consts = consts
        self.lines = []

    def cook(self, value):
        return re.sub(r"\$\{(\w+)\}", lambda m: self.consts.get(m.group(1), "${…}"), value)

    def line(self, value):
        self.lines.append(value)

    def items(self, items):
        for k, item in enumerate(items):
            if is_string(item):
                if k == 0 or items[k - 1] != (WORD, "from"):
                    self.line(self.cook(item[1]))
            elif is_group(item, "{"):
                self.object(item)
            elif is_group(item, "["):
                self.array(item)
            elif is_group(item):
                self.call(item, items[k - 1] if k else None)

    def call(self, group, callee):
        args = parts(group)
        if callee in ((WORD, "jsx"), (WORD, "jsxs")):
            args = args[1:2]  # the props: not the element's tag, nor its React key
        for arg in args:
            self.items(arg)

    def cell(self, element):
        """The text of a string or of an element of strings alone, or none."""
        if len(element) == 1 and is_string(element[0]):
            return self.cook(element[0][1])
        props = jsx_props(element)
        if props is None:
            return None
        values = [v for k, v in entries(props) if k != "className"]
        if all(len(v) == 1 and is_string(v[0]) for v in values):
            return " | ".join(self.cook(v[0][1]) for v in values)
        return None

    def object(self, obj):
        values = [(k, v) for k, v in entries(obj) if k != "className"]
        if values and all(len(v) == 1 and is_string(v[0]) for _, v in values):
            self.line(" | ".join(self.cook(v[0][1]) for _, v in values))
            return
        for key, value in values:
            if key == "children" and len(value) == 1 and is_group(value[0], "["):
                self.children(value[0])
            else:
                self.items(value)

    def array(self, group):
        elements = parts(group)
        cells = [self.cell(e) for e in elements]
        if cells and None not in cells:
            self.line(" | ".join(cells))
        else:
            for element in elements:
                self.items(element)

    def children(self, group):
        """An element's children: one line of prose if text is among them,
        its inline elements' text joined into it; else each on its own."""
        elements = parts(group)
        if not any(len(e) == 1 and is_string(e[0]) for e in elements):
            for element in elements:
                self.items(element)
            return
        inner = Text(self.consts)
        prose = []
        for element in elements:
            inner.lines = []
            inner.items(element)
            prose.append("".join(inner.lines))
        self.line("".join(prose))


def text(src):
    top = tree(src)
    consts = {}
    for k in range(len(top) - 3):
        word, eq, value, end = top[k : k + 4]
        if word[0] == WORD and eq == (MARK, "=") and is_string(value) and end in ((MARK, ","), (MARK, ";")):
            consts[word[1]] = value[1]
    page = Text(consts)
    page.items(top[1:])
    lines = (line.rstrip() for value in page.lines for line in value.split("\n"))
    return "\n".join(line for line in lines if line.strip()) + "\n"


def main():
    _, html = get(f"{SITE}/api-documentation/")
    index = one(r'src="(/assets/index-[\w-]+\.js)"', html, f"{SITE}/api-documentation/")
    bundle = module(SITE + index)
    here = Path(__file__).parent
    for name, copy in PAGES.items():
        chunk = one(rf'"\./({name}-[\w-]+\.js)"', bundle, index)
        (here / copy).write_text(text(module(f"{SITE}/assets/{chunk}")), encoding="utf-8")
        print(f"{chunk} -> {here / copy}")


if __name__ == "__main__":
    main()
