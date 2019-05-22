# pyre-strict
# misc utility functions...

from typing import Tuple


def strip_leading_whitespace(txt: str) -> Tuple[int, str]:
    # Given a multi-line string, determine the largest w such that each line
    # begins with at least w whitespace characters. Return w and the string
    # with w characters removed from the beginning of each line.
    lines = txt.split("\n")

    to_strip = None
    for line in lines:
        lsl = len(line.lstrip())
        if lsl:
            c = len(line) - lsl
            assert c >= 0
            if to_strip is None or to_strip > c:
                to_strip = c
            # TODO: do something about mixed tabs & spaces

    if not to_strip:
        return (0, txt)

    for i, line_i in enumerate(lines):
        if line_i.lstrip():
            lines[i] = line_i[to_strip:]

    return (to_strip, "\n".join(lines))
