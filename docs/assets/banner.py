"""Generates banner.svg: a rotating double helix, purple facing the viewer, cyan behind.

Usage: python3 docs/assets/banner.py docs/assets/banner.svg
"""
import math, sys
W, H = 1200, 300
CY, AMP = 150, 78
PERIOD = 300          # px per full turn
STEP = 4              # segment length
PURPLE = (168, 85, 247)
CYAN = (34, 211, 238)

BG = (11, 10, 22)

def mix(t, a=1.0):  # t=1 front (purple), t=0 back (cyan); a<1 fades into the background
    col = [c + (p - c) * t for p, c in zip(PURPLE, CYAN)]
    return "#%02x%02x%02x" % tuple(round(b + (v - b) * a) for v, b in zip(col, BG))

def strand(phase):
    back, front = [], []
    x = -PERIOD
    while x < W + PERIOD:
        th0 = 2 * math.pi * x / PERIOD + phase
        th1 = 2 * math.pi * (x + STEP) / PERIOD + phase
        y0, y1 = CY + AMP * math.sin(th0), CY + AMP * math.sin(th1)
        z = math.cos((th0 + th1) / 2)          # depth: +1 facing viewer
        t = (z + 1) / 2
        seg = (f'<line x1="{x}" y1="{y0:.1f}" x2="{x+STEP}" y2="{y1:.1f}" '
               f'stroke="{mix(t, 0.4 + 0.6*t)}" stroke-width="{4 + 6*t:.1f}" stroke-linecap="round"/>')
        (front if z >= 0 else back).append(seg)
        x += STEP
    return back, front

def rungs():
    out = []
    x = -PERIOD
    while x < W + PERIOD:
        th = 2 * math.pi * x / PERIOD
        ya, yb = CY + AMP * math.sin(th), CY - AMP * math.sin(th)
        ta, tb = (math.cos(th) + 1) / 2, (1 - math.cos(th)) / 2
        ym = CY
        if abs(ya - yb) > 10:
            out.append(f'<line x1="{x}" y1="{ya:.1f}" x2="{x}" y2="{ym}" stroke="{mix(ta, 0.2 + 0.4*ta)}" stroke-width="2.5"/>')
            out.append(f'<line x1="{x}" y1="{yb:.1f}" x2="{x}" y2="{ym}" stroke="{mix(tb, 0.2 + 0.4*tb)}" stroke-width="2.5"/>')
        x += PERIOD // 12
    return out

ab, af = strand(0)
bb, bf = strand(math.pi)
helix = "\n".join(ab + bb + rungs() + af + bf)

svg = f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" aria-label="helix trader">
  <title>helix trader</title>
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0" stop-color="#0d0718"/>
      <stop offset="1" stop-color="#07121a"/>
    </linearGradient>
    <radialGradient id="halo" cx="0.5" cy="0.5" r="0.5">
      <stop offset="0" stop-color="#0a0a14" stop-opacity="0.92"/>
      <stop offset="0.6" stop-color="#0a0a14" stop-opacity="0.75"/>
      <stop offset="1" stop-color="#0a0a14" stop-opacity="0"/>
    </radialGradient>
    <linearGradient id="fade" x1="0" x2="1">
      <stop offset="0" stop-color="#fff" stop-opacity="0"/>
      <stop offset="0.08" stop-color="#fff"/>
      <stop offset="0.92" stop-color="#fff"/>
      <stop offset="1" stop-color="#fff" stop-opacity="0"/>
    </linearGradient>
    <mask id="edges"><rect width="{W}" height="{H}" fill="url(#fade)"/></mask>
    <clipPath id="card"><rect width="{W}" height="{H}" rx="18"/></clipPath>
    <filter id="glow" x="-20%" y="-50%" width="140%" height="200%">
      <feGaussianBlur stdDeviation="6" result="b"/>
      <feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>
    </filter>
  </defs>
  <g clip-path="url(#card)">
    <rect width="{W}" height="{H}" fill="url(#bg)"/>
    <g mask="url(#edges)">
      <g filter="url(#glow)">
        <g>
{helix}
          <animateTransform attributeName="transform" type="translate" from="0 0" to="{-PERIOD} 0" dur="9s" repeatCount="indefinite"/>
        </g>
      </g>
    </g>
    <ellipse cx="{W/2}" cy="{CY}" rx="360" ry="110" fill="url(#halo)"/>
    <text x="{W/2}" y="{CY+22}" text-anchor="middle" font-family="ui-sans-serif, 'Segoe UI', Inter, Helvetica, Arial, sans-serif" font-size="68" font-weight="700" letter-spacing="2">
      <tspan fill="#c084fc">helix</tspan><tspan fill="#67e8f9" dx="18">trader</tspan>
    </text>
    <text x="{W/2}" y="{CY+62}" text-anchor="middle" font-family="ui-monospace, SFMono-Regular, Menlo, Consolas, monospace" font-size="17" fill="#a5b4c8" letter-spacing="3">SYSTEMATIC TRADING ON NAUTILUSTRADER · RUST</text>
  </g>
</svg>
'''
open(sys.argv[1], "w").write(svg)
