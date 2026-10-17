"""Set the README banner's text on its text-free illustrations.

light.png and dark.png in this directory are the illustrations, made with an
image model and holding no text. This script sets the text on them with
Pillow and writes ../banner-light.png and ../banner-dark.png, so changing a
line means editing LINES and running it again:

    pip install pillow
    python docs/assets/banner/compose.py path/to/InterVariable.ttf

The font is Inter's variable font (https://rsms.me/inter/, SIL Open Font
License). The script prints each line's contrast against the pixels behind
it; keep it at 4.5 or more for the small lines and 3 or more for the large.
"""
import pathlib
import sys

from PIL import Image, ImageDraw, ImageFont

HERE = pathlib.Path(__file__).resolve().parent
WIDTH, HEIGHT = 2064, 512
SCALE = 2  # set the text at twice the size, then scale down
X = 850  # left edge of the text

# Text, size (px), weight, optical size, tracking (em), baseline (px).
LINES = [
    ("igloo", 152, 700, 32, -0.025, 198),
    ("Open source SQL warehouse on Apache Iceberg", 47, 400, 32, -0.005, 308),
    ("5x faster • 30x cheaper per query*", 60, 700, 32, -0.01, 392),
    ("*at 10GB query scale, exact speedup and cost reduction will vary", 28, 450, 14, 0.0, 447),
]
COLORS = {
    "light": ["#15203F", "#46557A", "#B26400", "#55627F"],
    # The lines below the wordmark sit on moonlit snow, so they stay dark.
    "dark": ["#FFFFFF", "#2E3C60", "#9E5300", "#22304D"],
}


def load_font(path, size, weight, opsz):
    font = ImageFont.truetype(path, size * SCALE)
    values = []
    for axis in font.get_variation_axes():
        name = axis["name"]
        name = (name.decode() if isinstance(name, bytes) else name).lower()
        values.append(weight if name in ("weight", "wght") else opsz)
    font.set_variation_by_axes(values)
    return font


def luminance(rgb):
    def channel(v):
        v /= 255
        return v / 12.92 if v <= 0.03928 else ((v + 0.055) / 1.055) ** 2.4

    r, g, b = rgb[:3]
    return 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b)


def contrast(a, b):
    hi, lo = sorted((luminance(a), luminance(b)), reverse=True)
    return (hi + 0.05) / (lo + 0.05)


def compose(font_path, mode):
    image = Image.open(HERE / f"{mode}.png").convert("RGB")
    image = image.resize((WIDTH * SCALE, HEIGHT * SCALE), Image.LANCZOS)
    draw = ImageDraw.Draw(image)
    for (text, size, weight, opsz, tracking, baseline), color in zip(LINES, COLORS[mode]):
        font = load_font(font_path, size, weight, opsz)
        rgb = tuple(int(color[i:i + 2], 16) for i in (1, 3, 5))
        # Place each character at the length of the text before it, plus the
        # tracking, which Pillow's text() has no option for.
        positions = [(X * SCALE + font.getlength(text[:i]) + i * tracking * size * SCALE, baseline * SCALE)
                     for i in range(len(text))]
        boxes = [draw.textbbox(pos, ch, font=font, anchor="ls") for pos, ch in zip(positions, text)]
        box = (min(b[0] for b in boxes), min(b[1] for b in boxes), max(b[2] for b in boxes), max(b[3] for b in boxes))
        behind = image.crop(box).resize((64, 8))
        worst = min(contrast(rgb, behind.getpixel((x, y))) for x in range(64) for y in range(8))
        for pos, ch in zip(positions, text):
            draw.text(pos, ch, font=font, fill=color, anchor="ls")
        print(f"{mode:5s} {text[:40]:40s} contrast {worst:4.1f}, right edge {box[2] // SCALE:.0f}px")
    out = HERE.parent / f"banner-{mode}.png"
    image.resize((WIDTH, HEIGHT), Image.LANCZOS).save(out, optimize=True)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} path/to/InterVariable.ttf")
    for mode in COLORS:
        compose(sys.argv[1], mode)
