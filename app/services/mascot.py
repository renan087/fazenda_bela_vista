"""Pezinho, o mascote da IA do SiSFarm (pe de cafe no vaso de barro).

Desenho vetorial e animacoes (CSS dentro do SVG). Os arquivos em
app/static/img/pezinho/ sao gerados por este modulo:

    python -m app.services.mascot
"""
from __future__ import annotations

import math
from pathlib import Path

STATES = ("acenando", "pronto", "falando", "pensando", "alerta", "comemorando", "dormindo", "zangado")
HEAD_STATES = ("pronto", "pensando", "dormindo")
INK = "#141414"
SW = 12          # contorno
BROWN = "#8E5A2C"
LEAF = "#2E7D32"
VEIN = "#8DCB5E"
VB = "140 30 760 1450"
VB_HEAD = "150 30 720 960"

def leaf(B, T, w, droop=0, cls=""):
    bx, by = B; tx, ty = T
    L = math.hypot(tx - bx, ty - by)
    ang = math.degrees(math.atan2(tx - bx, -(ty - by)))
    ang += droop if ang > 0 else -droop
    body = (f'M0 0C{w * .62:.1f} {-L * .04:.1f} {w * .66:.1f} {-L * .62:.1f} 0 {-L:.1f}'
            f'C{-w * .66:.1f} {-L * .62:.1f} {-w * .62:.1f} {-L * .04:.1f} 0 0Z')
    veins = f'<path d="M0 {-L * .1:.1f}V{-L * .86:.1f}" stroke="{VEIN}" stroke-width="9" stroke-linecap="round"/>'
    for t in (.28, .47, .66):
        for s in (-1, 1):
            veins += f'<path d="M0 {-L * t:.1f}L{s * w * .3 * (1.1 - t * .75):.1f} {-L * (t + .12):.1f}" stroke="{VEIN}" stroke-width="8" stroke-linecap="round"/>'
    return (f'<g class="{cls}" transform="translate({bx} {by}) rotate({ang:.1f})">'
            f'<path d="{body}" fill="{LEAF}" stroke="{INK}" stroke-width="{SW}" stroke-linejoin="round"/>{veins}</g>')

def cherry(x, y, r=40, cls="pz-cherry"):
    return (f'<g class="{cls}"><circle cx="{x}" cy="{y}" r="{r}" fill="#E0312F" stroke="{INK}" stroke-width="{SW}"/>'
            f'<ellipse cx="{x - r * .32:.1f}" cy="{y - r * .36:.1f}" rx="{r * .3:.1f}" ry="{r * .2:.1f}" fill="#fff" transform="rotate(-25 {x - r * .32:.1f} {y - r * .36:.1f})"/></g>')

def branch(d, w=14):
    return (f'<path d="{d}" fill="none" stroke="{INK}" stroke-width="{w + 2 * SW - 4}" stroke-linecap="round" stroke-linejoin="round"/>'
            f'<path d="{d}" fill="none" stroke="{BROWN}" stroke-width="{w}" stroke-linecap="round" stroke-linejoin="round"/>')

def glove(x, y, ang, cls=""):
    parts = [("rect", -46, -58, 92, 64, 30),                 # palma
             ("rect", -44, -112, 24, 74, 12), ("rect", -18, -124, 24, 86, 12),
             ("rect", 8, -120, 24, 82, 12), ("rect", 32, -100, 22, 64, 11)]
    thumb = '<rect x="-70" y="-44" width="26" height="54" rx="13" transform="rotate(-38 -57 -17)"/>'
    shapes = "".join(f'<rect x="{a}" y="{b}" width="{c}" height="{d}" rx="{e}"/>' for _, a, b, c, d, e in parts) + thumb
    cuff = f'<rect x="-40" y="-6" width="80" height="34" rx="14" fill="#fff" stroke="{INK}" stroke-width="{SW - 2}"/>'
    lines = f'<path d="M-20 -50v-22M6 -50v-28M31 -48v-18" stroke="{INK}" stroke-width="7" stroke-linecap="round"/>'
    return (f'<g class="{cls}" transform="translate({x} {y}) rotate({ang}) scale(.8)">'
            f'<g fill="{INK}" stroke="{INK}" stroke-width="{SW * 2 - 4}" stroke-linejoin="round">{shapes}</g>'
            f'<g fill="#fff">{shapes}</g>{lines}{cuff}</g>')

def fist(x, y, ang, cls=""):
    return (f'<g class="{cls}" transform="translate({x} {y}) rotate({ang}) scale(.8)">'
            f'<rect x="-48" y="-62" width="96" height="70" rx="32" fill="#fff" stroke="{INK}" stroke-width="{SW}"/>'
            f'<path d="M-30 -36h40M-30 -16h46" stroke="{INK}" stroke-width="7" stroke-linecap="round"/>'
            f'<path d="M30 -54q22 10 10 34" fill="#fff" stroke="{INK}" stroke-width="{SW - 2}" stroke-linecap="round"/>'
            f'<rect x="-40" y="2" width="80" height="32" rx="14" fill="#fff" stroke="{INK}" stroke-width="{SW - 2}"/></g>')

def knock_fist(x, y):
    knuckles = "".join(f'<circle cx="{k}" cy="-22" r="24"/>' for k in (-50, -17, 17, 50))
    shape = knuckles + '<rect x="-72" y="-26" width="144" height="74" rx="34"/>'
    return (f'<g transform="translate({x} {y}) scale(1.45)">'
            f'<g fill="{INK}" stroke="{INK}" stroke-width="{SW * 2 - 2}">{shape}</g><g fill="#fff">{shape}</g>'
            f'<path d="M-34 -14v16M0 -16v18M34 -14v16" stroke="{INK}" stroke-width="7" stroke-linecap="round"/>'
            f'<path d="M-52 26Q-6 44 44 26" fill="none" stroke="{INK}" stroke-width="8" stroke-linecap="round"/>'
            f'<path d="M-46 -34q8 -10 18 -6M-12 -36q8 -10 18 -6" stroke="#D9DDE2" stroke-width="6" stroke-linecap="round" fill="none"/></g>')

def arm(d, cls=""):
    return f'<g class="{cls}">{branch(d, 16)}</g>' if cls else branch(d, 16)

def draw(state="acenando", uid="p", head=False):
    s = state
    droop = {"dormindo": 16, "comemorando": -10, "alerta": -8}.get(s, 0)
    leaves = (leaf((405, 448), (163, 322), 178, droop, "pz-ll") + leaf((590, 444), (826, 298), 178, droop, "pz-lr") +
              leaf((450, 548), (196, 628), 150, droop, "pz-ll2") + leaf((550, 548), (793, 606), 146, droop, "pz-lr2") +
              leaf((495, 372), (497, 56), 190, 0, "pz-lc"))
    branches = (branch("M494 372C488 450 500 520 494 600C490 660 492 712 490 760", 16) +
                branch("M493 528C470 505 440 488 405 470") + branch("M492 580C472 566 460 556 448 546") +
                branch("M497 486C530 466 565 452 600 444") + branch("M494 590C515 574 535 560 554 547"))
    cherries = cherry(408, 470, 42) + cherry(612, 497, 38, "pz-cherry pz-c2") + cherry(672, 456, 38)
    # olhos: maiores e mais ovais que a referência
    px, py = {"pensando": (14, -22), "alerta": (-6, 2), "dormindo": (0, 0)}.get(s, (6, -8))
    if s == "zangado":
        eyes = (f'<defs><clipPath id="pzAngL"><path d="M320 744L500 790V900H320Z"/></clipPath><clipPath id="pzAngR"><path d="M470 786L650 738V900H470Z"/></clipPath></defs>'
                f'<g clip-path="url(#pzAngL)"><ellipse cx="418" cy="786" rx="62" ry="80" fill="#fff" stroke="{INK}" stroke-width="{SW}"/><ellipse cx="434" cy="812" rx="25" ry="31" fill="{INK}"/><circle cx="442" cy="800" r="8" fill="#fff"/></g>'
                f'<g clip-path="url(#pzAngR)"><ellipse cx="560" cy="780" rx="62" ry="80" fill="#fff" stroke="{INK}" stroke-width="{SW}"/><ellipse cx="546" cy="806" rx="25" ry="31" fill="{INK}"/><circle cx="554" cy="794" r="8" fill="#fff"/></g>'
                f'<path d="M362 752L478 782M498 780L616 750" stroke="{INK}" stroke-width="{SW}" stroke-linecap="round"/>')
    elif s == "dormindo":
        eyes = f'<path d="M362 786q56 40 112 0M504 780q56 40 112 0" fill="none" stroke="{INK}" stroke-width="13" stroke-linecap="round"/>'
    elif s == "comemorando":
        eyes = f'<path d="M366 806l52-62 52 62M508 800l52-62 52 62" fill="none" stroke="{INK}" stroke-width="14" stroke-linecap="round" stroke-linejoin="round"/>'
    else:
        k = 1.08 if s == "alerta" else 1
        eyes = '<g class="pz-eyes">' + "".join(
            f'<ellipse cx="{cx}" cy="{cy}" rx="{62 * k:.0f}" ry="{80 * k:.0f}" fill="#fff" stroke="{INK}" stroke-width="{SW}"/>'
            f'<g class="pz-pupil"><ellipse cx="{cx + 14 + px}" cy="{cy + 6 + py}" rx="31" ry="41" fill="{INK}"/>'
            f'<ellipse cx="{cx + 24 + px}" cy="{cy - 12 + py}" rx="11" ry="13" fill="#fff"/></g>'
            for cx, cy in ((418, 786), (560, 780))) + '</g>'
    by = -14 if s == "alerta" else 0
    if s == "zangado":
        brows = f'<path class="pz-brows" d="M356 706L476 752M502 750L624 700" fill="none" stroke="{INK}" stroke-width="20" stroke-linecap="round"/>'
    elif s == "dormindo":
        brows = ""
    else:
        brows = (f'<path class="pz-brows" d="M368 {690 + by}Q404 {656 + by} 446 {676 + by}M540 {670 + by}Q582 {644 + by} 618 {674 + by}" fill="none" stroke="{INK}" stroke-width="13" stroke-linecap="round"/>')
    tongue = '<path d="M478 934Q500 918 524 932Q514 948 500 948Q486 948 478 934Z" fill="#F28AA0"/>'
    mouth = {
        "acenando": f'<path d="M458 900Q500 890 544 896Q536 952 500 952Q464 950 458 900Z" fill="#1E0E0E" stroke="{INK}" stroke-width="8" stroke-linejoin="round"/>{tongue}',
        "pronto": f'<path d="M462 902Q500 896 540 900Q532 946 500 946Q468 944 462 902Z" fill="#1E0E0E" stroke="{INK}" stroke-width="8" stroke-linejoin="round"/>{tongue}',
        "falando": f'<g class="pz-talk"><path d="M458 898Q500 890 544 894Q538 962 500 962Q462 960 458 898Z" fill="#1E0E0E" stroke="{INK}" stroke-width="8" stroke-linejoin="round"/><path d="M476 944Q500 926 526 942Q516 958 500 958Q484 958 476 944Z" fill="#F28AA0"/></g>',
        "pensando": f'<path d="M470 922Q500 908 532 920" fill="none" stroke="{INK}" stroke-width="12" stroke-linecap="round"/>',
        "alerta": f'<path d="M462 904Q500 898 538 902Q530 950 500 950Q470 948 462 904Z" fill="#1E0E0E" stroke="{INK}" stroke-width="8" stroke-linejoin="round"/>{tongue}',
        "comemorando": f'<path d="M446 890Q502 878 558 888Q548 972 502 972Q456 970 446 890Z" fill="#1E0E0E" stroke="{INK}" stroke-width="8" stroke-linejoin="round"/><path d="M472 948Q502 924 534 946Q520 966 502 966Q484 966 472 948Z" fill="#F28AA0"/>',
        "dormindo": f'<path d="M482 918Q502 930 522 918" fill="none" stroke="{INK}" stroke-width="11" stroke-linecap="round"/>',
        "zangado": (f'<path d="M456 940Q500 912 546 940L546 954Q500 944 456 954Z" fill="#fff" stroke="{INK}" stroke-width="9" stroke-linejoin="round"/>'
                    f'<path d="M458 947Q500 928 544 947M478 932v18M500 926v20M522 932v18" fill="none" stroke="{INK}" stroke-width="6" stroke-linecap="round"/>'
                    f'<g class="pz-blush"><ellipse cx="370" cy="884" rx="34" ry="18" fill="#F25C54" opacity=".55"/><ellipse cx="612" cy="878" rx="34" ry="18" fill="#F25C54" opacity=".55"/></g>'),
    }[s]
    # braços
    rest_l = arm("M505 990C450 965 392 978 368 1040") + glove(352, 1062, 172)
    rest_r = arm("M510 990C566 966 620 980 640 1040") + glove(656, 1062, -172)
    if s == "acenando":
        arms = rest_l + f'<g class="pz-wave">{arm("M510 990C592 980 652 934 690 852")}{glove(704, 832, 22)}</g>'
    elif s == "falando":
        arms = rest_l + f'<g class="pz-gesture">{arm("M510 990C592 994 646 968 676 918")}{glove(694, 896, 58)}</g>'
    elif s == "pensando":
        arms = rest_l + arm("M510 990C600 996 626 950 598 922") + glove(578, 930, -48)
    elif s == "alerta":
        arms = rest_l + arm("M510 990C590 990 636 960 660 914") + (f'<g class="pz-knock">{knock_fist(662, 896)}'
                         f'<g class="pz-impact" stroke="{INK}" stroke-width="9" stroke-linecap="round"><path d="M534 782l-30 -32M662 754v-46M790 782l30 -32M834 896h46M490 896h-46"/></g></g>')
    elif s == "comemorando":
        arms = (f'<g class="pz-cheer-l">{arm("M505 990C420 965 356 880 336 770")}{glove(332, 744, -14)}</g>'
                f'<g class="pz-cheer-r">{arm("M510 990C594 965 658 880 678 770")}{glove(682, 744, 14)}</g>')
    elif s == "zangado":
        arms = (f'<g class="pz-shake-l">{arm("M505 990C424 1004 350 988 324 940")}{fist(312, 914, -18)}</g>'
                f'<g class="pz-shake-r">{arm("M510 990C590 1004 664 986 690 936")}{fist(702, 908, 18)}</g>')
    else:
        arms = rest_l + rest_r
    lower = branch("M508 1088C506 1050 510 1010 507 962", 16) + f'<path d="M488 958H528" stroke="{INK}" stroke-width="10" stroke-linecap="round"/>'
    deco = ""
    if s == "acenando":
        deco = f'<g class="pz-lines" fill="none" stroke="{INK}" stroke-width="9" stroke-linecap="round"><path d="M800 700Q826 720 830 760"/><path d="M826 682Q860 714 862 772"/><path d="M806 744Q814 760 812 782"/></g>'
    if s == "pensando":
        deco = '<g class="pz-think"><circle cx="666" cy="772" r="14" fill="#C8D3C4"/><circle cx="712" cy="730" r="21" fill="#C8D3C4"/><circle cx="778" cy="696" r="30" fill="#C8D3C4"/></g>'
    if s == "alerta":
        deco = ('<g class="pz-toc"><text x="700" y="690" font-family="Inter,sans-serif" font-weight="900" font-size="92" fill="#E39A12" stroke="#141414" stroke-width="10" paint-order="stroke" transform="rotate(-12 700 690)">toc</text></g>'
                '<g class="pz-toc pz-toc2"><text x="760" y="1010" font-family="Inter,sans-serif" font-weight="900" font-size="84" fill="#E39A12" stroke="#141414" stroke-width="10" paint-order="stroke" transform="rotate(8 760 1010)">toc!</text></g>')
    if s == "comemorando":
        deco = ('<g class="pz-spark"><path d="M214 760l14 32 32 14-32 14-14 32-14-32-32-14 32-14z" fill="#F2B632"/></g>'
                '<g class="pz-spark pz-s2"><path d="M820 720l11 25 25 11-25 11-11 25-11-25-25-11 25-11z" fill="#8DCB5E"/></g>'
                '<g class="pz-spark pz-s3"><circle cx="250" cy="930" r="14" fill="#E0312F"/><circle cx="790" cy="930" r="12" fill="#F2B632"/></g>')
    if s == "zangado":
        puff = lambda x, y: f'<g><circle cx="{x}" cy="{y}" r="26" fill="#fff" stroke="{INK}" stroke-width="8"/><circle cx="{x + 26}" cy="{y - 14}" r="21" fill="#fff" stroke="{INK}" stroke-width="8"/><circle cx="{x + 4}" cy="{y - 26}" r="16" fill="#fff" stroke="{INK}" stroke-width="8"/></g>'
        deco = (f'<g class="pz-steam">{puff(286, 800)}</g><g class="pz-steam pz-steam2">{puff(676, 792)}</g>'
                '<g class="pz-vein"><path d="M680 688q18 -4 22 -24M716 676q4 18 24 22M728 712q-18 4 -22 24M692 724q-4 -18 -24 -22" fill="none" stroke="#E0312F" stroke-width="13" stroke-linecap="round"/></g>')
    if s == "dormindo":
        deco = ('<g class="pz-z"><text x="700" y="700" font-family="Inter,sans-serif" font-weight="800" font-size="70" fill="#7A857E">z</text></g>'
                '<g class="pz-z pz-z2"><text x="760" y="620" font-family="Inter,sans-serif" font-weight="800" font-size="92" fill="#7A857E">z</text></g>')
    pot_back = f'''<rect x="352" y="1400" width="290" height="60" rx="16" fill="#E8682E" stroke="{INK}" stroke-width="{SW}"/>
<path d="M292 1164C268 1250 298 1356 362 1414H632C696 1356 724 1250 700 1164Z" fill="#E8682E" stroke="{INK}" stroke-width="{SW}" stroke-linejoin="round"/>
<path d="M298 1172H696L699 1194Q500 1206 295 1194Z" fill="#C9541F"/>
<rect x="326" y="1206" width="22" height="84" rx="11" fill="#fff" opacity=".92"/><circle cx="337" cy="1314" r="10" fill="#fff" opacity=".92"/>
<rect x="262" y="1054" width="468" height="120" rx="54" fill="#F07B38" stroke="{INK}" stroke-width="{SW}"/>
<rect x="624" y="1118" width="54" height="18" rx="9" fill="#fff" opacity=".92"/>
<path d="M332 1088Q500 1040 672 1084Q676 1100 660 1104Q500 1094 344 1104Q328 1100 332 1088Z" fill="#6A3518" stroke="{INK}" stroke-width="9" stroke-linejoin="round"/>'''
    plant = f'<g class="pz-plant pz-{s}">{branches}{leaves}{cherries}{lower}{eyes}{brows}{mouth}{arms}</g>'
    if head:
        return f'{branches}{leaves}{cherries}{eyes}{brows}{mouth}'
    return f'<ellipse cx="497" cy="1468" rx="190" ry="14" fill="#16201A" opacity=".1"/>{pot_back}{plant}{deco}'


CSS = """
.pz-root{transform-box:view-box;transform-origin:500px 1460px}
.pz-plant,.pz-wave,.pz-cheer-l,.pz-cheer-r,.pz-gesture{transform-box:view-box}
.pz-plant{transform-origin:505px 1090px;animation:pz-sway 4.2s ease-in-out infinite}
.pz-eyes{transform-box:fill-box;transform-origin:center;animation:pz-blink 4.8s infinite}
.pz-cherry{transform-box:fill-box;transform-origin:50% 0;animation:pz-swing 3s ease-in-out infinite}
.pz-c2{animation-delay:-1.2s}
.pz-wave{transform-origin:510px 990px;animation:pz-wave .9s ease-in-out infinite}
.pz-lines{animation:pz-fade .9s ease-in-out infinite}
.pz-talk{transform-box:fill-box;transform-origin:50% 0;animation:pz-talk .32s ease-in-out infinite alternate}
.pz-gesture{transform-origin:510px 990px;animation:pz-gest 1.6s ease-in-out infinite}
.pz-pensando .pz-pupil{animation:pz-look 3s ease-in-out infinite}
.pz-think circle{animation:pz-pulse 1.5s ease-in-out infinite}
.pz-think circle:nth-child(2){animation-delay:.25s}.pz-think circle:nth-child(3){animation-delay:.5s}
.pz-knock{transform-box:view-box;transform-origin:662px 896px;animation:pz-knock 1.5s ease-in infinite}
.pz-impact{opacity:0;animation:pz-impact 1.5s linear infinite}
.pz-alerta{animation:pz-lean 1.5s ease-in infinite}
.pz-toc{opacity:0;transform-box:fill-box;transform-origin:center;animation:pz-toc 1.5s linear infinite}
.pz-toc2{animation-name:pz-toc2}
@keyframes pz-knock{0%,30%{transform:translate(0,10px) scale(.55)}44%{transform:translate(24px,0) scale(1.22)}50%{transform:translate(14px,6px) scale(.92)}58%{transform:translate(24px,0) scale(1.24)}68%,100%{transform:translate(0,10px) scale(.55)}}
@keyframes pz-impact{0%,42%{opacity:0}44%{opacity:1}49%{opacity:0}56%{opacity:0}58%{opacity:1}64%,100%{opacity:0}}
@keyframes pz-lean{0%,30%,70%,100%{transform:rotate(0)}44%,58%{transform:rotate(2.5deg) scale(1.02)}}
@keyframes pz-toc{0%,42%{opacity:0;transform:scale(.6)}45%{opacity:1;transform:scale(1.1)}80%{opacity:1;transform:scale(1)}90%,100%{opacity:0}}
@keyframes pz-toc2{0%,56%{opacity:0;transform:scale(.6)}59%{opacity:1;transform:scale(1.1)}86%{opacity:1;transform:scale(1)}94%,100%{opacity:0}}
.pz-cheer-l{transform-origin:505px 990px;animation:pz-cheer-l .8s ease-in-out infinite}
.pz-cheer-r{transform-origin:510px 990px;animation:pz-cheer-r .8s ease-in-out infinite}
.pz-comemorando .pz-cherry{animation:pz-bounce .4s ease-in-out infinite alternate}
.pz-spark{transform-box:fill-box;transform-origin:center;animation:pz-twinkle 1.2s ease-in-out infinite}
.pz-s2{animation-delay:.4s}.pz-s3{animation-delay:.8s}
.pz-dormindo{animation:pz-breathe 3.6s ease-in-out infinite}
.pz-z{animation:pz-float 2.8s ease-in infinite;opacity:0}
.pz-z2{animation-delay:1.4s}
@keyframes pz-sway{0%,100%{transform:rotate(-1.6deg)}50%{transform:rotate(1.6deg)}}
@keyframes pz-blink{0%,93%,100%{transform:scaleY(1)}96%{transform:scaleY(.08)}}
@keyframes pz-swing{0%,100%{transform:rotate(-6deg)}50%{transform:rotate(6deg)}}
@keyframes pz-wave{0%,100%{transform:rotate(0)}50%{transform:rotate(-16deg)}}
@keyframes pz-fade{0%,100%{opacity:.2}50%{opacity:1}}
@keyframes pz-talk{from{transform:scaleY(1)}to{transform:scaleY(.45)}}
@keyframes pz-gest{0%,100%{transform:rotate(0)}50%{transform:rotate(-9deg)}}
@keyframes pz-look{0%,100%{transform:translate(0,0)}50%{transform:translate(-14px,4px)}}
@keyframes pz-pulse{0%,100%{opacity:.35}50%{opacity:1}}
@keyframes pz-pop{0%,100%{transform:scale(1)}50%{transform:scale(1.15)}}
@keyframes pz-jump{0%,100%{transform:translateY(0)}45%{transform:translateY(-80px)}}
@keyframes pz-cheer-l{0%,100%{transform:rotate(0)}50%{transform:rotate(10deg)}}
@keyframes pz-cheer-r{0%,100%{transform:rotate(0)}50%{transform:rotate(-10deg)}}
@keyframes pz-bounce{from{transform:translateY(0)}to{transform:translateY(-12px)}}
@keyframes pz-twinkle{0%,100%{transform:scale(.6);opacity:.4}50%{transform:scale(1.15);opacity:1}}
@keyframes pz-breathe{0%,100%{transform:rotate(3deg) translateY(0)}50%{transform:rotate(3deg) translateY(8px)}}
@keyframes pz-float{0%{opacity:0;transform:translate(0,20px)}25%{opacity:1}100%{opacity:0;transform:translate(30px,-70px)}}
.pz-zangado{animation:pz-rage .18s linear infinite}
.pz-shake-l,.pz-shake-r{transform-box:view-box}
.pz-shake-l{transform-origin:505px 990px;animation:pz-fist-l .3s ease-in-out infinite}
.pz-shake-r{transform-origin:510px 990px;animation:pz-fist-r .3s ease-in-out infinite}
.pz-blush{animation:pz-pulse 1s ease-in-out infinite}
.pz-steam{animation:pz-steam 1.6s ease-out infinite;opacity:0}
.pz-steam2{animation-delay:.8s}
.pz-vein{transform-box:fill-box;transform-origin:center;animation:pz-pop .5s ease-in-out infinite}
@keyframes pz-rage{0%,100%{transform:translateX(0)}25%{transform:translateX(-6px) rotate(-.6deg)}75%{transform:translateX(6px) rotate(.6deg)}}
@keyframes pz-fist-l{0%,100%{transform:rotate(0)}50%{transform:rotate(9deg)}}
@keyframes pz-fist-r{0%,100%{transform:rotate(0)}50%{transform:rotate(-9deg)}}
@keyframes pz-steam{0%{opacity:0;transform:translateY(20px) scale(.6)}30%{opacity:1}100%{opacity:0;transform:translateY(-90px) scale(1.15)}}
@media (prefers-reduced-motion:reduce){.pz-root,.pz-root *{animation:none!important}}
.pz-root.is-comemorando{animation:pz-jump .8s ease-in-out infinite}
"""


def mascot_svg(state: str = "pronto", head: bool = False) -> str:
    """SVG completo e animado do Pezinho num estado."""
    if state not in STATES:
        raise ValueError(f"estado desconhecido: {state}")
    view_box = VB_HEAD if head else VB
    width, height = (720, 960) if head else (760, 1450)
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{view_box}" width="{width}" height="{height}" role="img" '
        f'aria-label="Pezinho, assistente SiSFarm"><style>{CSS}</style>'
        f'<g class="pz-root is-{state}">{draw(state, head=head)}</g></svg>'
    )


def build(directory: Path | None = None) -> list[Path]:
    """Gera os arquivos estaticos (corpo inteiro e so a cabeca para avatar)."""
    out = directory or Path(__file__).resolve().parents[1] / "static" / "img" / "pezinho"
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for state in STATES:
        path = out / f"{state}.svg"
        path.write_text(mascot_svg(state), encoding="utf-8")
        written.append(path)
    for state in HEAD_STATES:
        path = out / f"cabeca-{state}.svg"
        path.write_text(mascot_svg(state, head=True), encoding="utf-8")
        written.append(path)
    return written


if __name__ == "__main__":
    for item in build():
        print(item)
