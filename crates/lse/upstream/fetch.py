"""Copies the text of LSE's documentation beside this script.

LSE's site is a JavaScript bundle loading a module for each page. A page is
documentation when it is laid out by the site's documentation kit (its
module imports DocKit and gives the layout a `rail`, its sidebar of
contents): the API and WebSocket documentation, and the terms and licence,
on 14 November 2026. Every module the bundle loads is read to find them, so
a page added is copied and the copy of a page removed is deleted.

A page's module is read as text and never run: each string of the page, a
paragraph a line, its inline code joined into it; table rows and objects of
strings as cells joined by " | "; class names, element tags, React keys and
module paths left out, so that a redeploy that changes no text changes no
line. Each copy is named by its page's module.

Usage: python3 crates/lse/upstream/fetch.py
Then `git diff crates/lse/upstream/` shows what LSE changed since the copies
committed last (see the crate's README, "LSE's documentation").
"""
import re
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

SITE = "https://londonstrategicedge.com"
# A page laid out by the documentation kit, and its module's name.
DOCUMENT = re.compile(r'from"\./DocKit-[\w-]+\.js".*\brail:', re.S)
MODULE = re.compile(r"(.+)-[\w-]{8}\.js")


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


# Words after which a slash opens a regular expression, not a division.
BEFORE_REGEX = {"return", "typeof", "case", "do", "else", "in", "of", "new", "delete", "void", "throw", "instanceof", "yield", "await"}


def tokens(src, i=0):
    """The tokens of `src` from `i`, each with where it ends."""
    n, prev = len(src), None
    while i < n:
        c = src[i]
        if c.isspace():
            i += 1
            continue
        if src.startswith("//", i):
            end = src.find("\n", i)
            i = n if end < 0 else end
            continue
        if src.startswith("/*", i):
            i = src.index("*/", i) + 2
            continue
        if c in "\"'`":
            value, i = string(src, i)
            prev = STRING, value
        elif c.isalnum() or c in "_$":
            j = i
            while j < n and (src[j].isalnum() or src[j] in "_$"):
                j += 1
            prev = WORD, src[i:j]
            i = j
        elif c == "/" and (
            prev is None
            or prev[0] == MARK and prev[1] not in ")]}"
            or prev[0] == WORD and prev[1] in BEFORE_REGEX
        ):
            i = regex(src, i)
            continue  # a pattern is no text of the page
        else:
            prev = MARK, c
            i += 1
        yield *prev, i


def regex(src, i):
    """Where the regular expression opening at `i` ends, its flags with it."""
    i, in_class = i + 1, False
    while in_class or src[i] != "/":
        if src[i] == "\\":
            i += 1
        elif src[i] in "[]":
            in_class = src[i] == "["
        i += 1
    i += 1
    while i < len(src) and src[i].isalpha():
        i += 1
    return i


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
            depth = 1
            for kind, value, end in tokens(src, i + 2):
                if kind == MARK and value in "{}":
                    depth += 1 if value == "{" else -1
                    if not depth:
                        break
            out.append(src[i:end])
            i = end
        else:
            out.append(c)
            i += 1
    return "".join(out), i + 1


def tree(src):
    """The tokens of `src` nested by their brackets: each bracketed run a
    list opening with its bracket."""
    stack = [["("]]
    for kind, value, _ in tokens(src):
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
        """`value` with each substitution of a constant string by the string,
        any other by `${…}`, its expression's names being the build's."""
        return re.sub(r"\$\{([^`]*?)\}", lambda m: self.consts.get(m.group(1), "${…}"), value)

    def line(self, value):
        self.lines.append(value)

    def items(self, items):
        for k, item in enumerate(items):
            if is_string(item):
                if k == 0 or items[k - 1] not in ((WORD, "from"), (WORD, "import")):
                    self.line(self.cook(item[1]))
            elif is_group(item, "{"):
                self.object(item)
            elif is_group(item, "["):
                self.array(item)
            elif is_group(item):
                self.call(item, items[k - 1] if k else None)

    def call(self, group, callee):
        args = parts(group)
        if callee == (WORD, "import"):
            return  # a module's path
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
    values = (value for value in page.lines if re.search(r"\w", value.replace("${…}", "")))
    lines = (line.rstrip() for value in values for line in value.split("\n"))
    return "\n".join(line for line in lines if line.strip()) + "\n"


def main():
    _, html = get(f"{SITE}/")
    index = one(r'src="(/assets/index-[\w-]+\.js)"', html, SITE)
    files = sorted(set(re.findall(r'import\("\./([\w-]+\.js)"\)', module(SITE + index))))
    with ThreadPoolExecutor(8) as pool:
        sources = pool.map(lambda file: module(f"{SITE}/assets/{file}"), files)
        documents = {f: src for f, src in zip(files, sources) if DOCUMENT.search(src)}
    if not documents:
        sys.exit(f"none of the {len(files)} modules of {index} is a page of LSE's documentation kit")
    here = Path(__file__).parent
    copies = {here / f"{MODULE.fullmatch(f).group(1)}.txt": text(src) for f, src in documents.items()}
    for stale in set(here.glob("*.txt")) - copies.keys():
        stale.unlink()
        print(f"{stale.name}: no longer a page of LSE's documentation, deleted")
    for copy, page in sorted(copies.items()):
        copy.write_text(page, encoding="utf-8")
        print(copy.name)


if __name__ == "__main__":
    main()
