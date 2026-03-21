"""
Formatter — форматирует алерты в твиты и Telegram сообщения.

Стиль 2026:
- Без хэштегов (Twitter их депромоутирует)
- Ссылка убрана из основного твита — постится отдельным reply
- Чистый, информативный текст
- Небольшой комментарий для контекста
"""


# ─── Router ──────────────────────────────────────────────────────────────────

def format_alert(alert: dict) -> str | None:
    atype = alert.get("type")
    dispatch = {
        "WHALE_XLM":          _whale_xlm,
        "WHALE_USDC":         _whale_usdc,
        "SDEX":               _sdex,
        "SDEX_MULTI":         _sdex_multi,
        "VOLUME_SPIKE":       _volume_spike,
        "CROSS_BORDER":       _cross_border,
        "SOROBAN_LIQUIDITY":  _soroban_liquidity,
        "SOROBAN_SWAP":       _soroban_swap,
        "SOROBAN_NEW_POOL":   _soroban_new_pool,
        "MEME_PUMP":          _meme_pump,
        "AIRDROP_SUMMARY":    _airdrop_summary,
    }
    fn = dispatch.get(atype)
    return _trim(fn(alert)) if fn else None


def format_reply(alert: dict) -> str | None:
    """Возвращает текст reply-комментария со ссылкой."""
    link = alert.get("link", "")
    if not link:
        return None
    return f"🔍 Transaction details:\n{link}"


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _usd(value: float) -> str:
    if value >= 1_000_000_000:
        return f"${value/1_000_000_000:.2f}B"
    elif value >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    elif value >= 1_000:
        return f"${value/1_000:.0f}K"
    return f"${value:.0f}"


def _fmt_number(n: float) -> str:
    if n >= 1_000_000_000:
        return f"{n/1_000_000_000:.2f}B"
    elif n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    elif n >= 1_000:
        return f"{n/1_000:.0f}K"
    return str(int(n))


def _pct(p: float) -> str:
    return f"+{p:.0f}%"


def _fdv_str(fdv: float) -> str:
    if fdv >= 1_000_000:
        return f"Market cap: {_usd(fdv)}"
    return f"Market cap: {_usd(fdv)}"


def _intensity(usd: float) -> str:
    """Визуальная интенсивность по объёму — стиль Whale Alert."""
    if usd >= 50_000_000:
        return "🚨 " * 5
    elif usd >= 10_000_000:
        return "🔴 " * 4
    elif usd >= 1_000_000:
        return "🟠 " * 3
    elif usd >= 250_000:
        return "🟡 " * 2
    else:
        return "🔵"


def _whale_label(usd: float) -> str:
    if usd >= 50_000_000:
        return "HISTORIC"
    elif usd >= 10_000_000:
        return "WHALE"
    elif usd >= 1_000_000:
        return "BIG MOVE"
    elif usd >= 250_000:
        return "NOTABLE"
    return "ALERT"


def _move_context(move_ctx: str | None) -> str:
    """Читаемый контекст движения средств."""
    if not move_ctx:
        return ""
    labels = {
        "Exchange → Exchange": "Exchange to Exchange transfer",
        "Exchange Outflow": "Leaving exchange",
        "Exchange Inflow": "Moving to exchange",
    }
    return labels.get(move_ctx, move_ctx)


# ─── Whale XLM ────────────────────────────────────────────────────────────────

def _whale_xlm(a):
    usd = a["usd_value"]
    intensity = _intensity(usd).strip()
    label = _whale_label(usd)
    move_ctx = _move_context(a.get("move_ctx"))

    ctx_line = f"↳ {move_ctx}\n" if move_ctx else ""

    from_label = a['from']
    to_label   = a['to']
    from_line  = f"From  {from_label}\n" if from_label else ""
    to_line    = f"To    {to_label}"     if to_label   else ""

    return (
        f"{intensity} {label}\n\n"
        f"{_fmt_number(a['amount'])} XLM moved on Stellar\n"
        f"Worth {_usd(usd)}\n\n"
        f"{ctx_line}"
        f"{from_line}"
        f"{to_line}"
    ).rstrip()


# ─── Whale USDC ───────────────────────────────────────────────────────────────

def _whale_usdc(a):
    usd = a["usd_value"]
    intensity = _intensity(usd).strip()
    label = _whale_label(usd)
    asset = a.get("asset", "USDC")
    move_ctx = _move_context(a.get("move_ctx"))

    ctx_line = f"↳ {move_ctx}\n" if move_ctx else ""

    from_label = a['from']
    to_label   = a['to']
    from_line  = f"From  {from_label}\n" if from_label else ""
    to_line    = f"To    {to_label}"     if to_label   else ""

    return (
        f"{intensity} {label}\n\n"
        f"{_fmt_number(a['amount'])} {asset} transferred on Stellar\n"
        f"Worth {_usd(usd)}\n\n"
        f"{ctx_line}"
        f"{from_line}"
        f"{to_line}"
    ).rstrip()


# ─── SDEX Trade ───────────────────────────────────────────────────────────────

def _sdex(a):
    usd = a["usd_value"]
    intensity = _intensity(usd).strip()
    is_mm = a.get("is_market_maker", False)
    selling = a.get("selling", "?")
    buying  = a.get("buying", "?")
    amount  = a.get("amount", 0)

    mm_note = "\nMarket maker activity detected" if is_mm else ""

    return (
        f"{intensity} Large DEX Trade\n\n"
        f"{_fmt_number(amount)} {selling} → {buying}\n"
        f"Volume: {_usd(usd)} on Stellar DEX"
        f"{mm_note}"
    )


# ─── Cross-Border Payment ─────────────────────────────────────────────────────

def _cross_border(a):
    usd = a["usd_value"]
    intensity = _intensity(usd).strip()
    src = a["src_asset"]
    dst = a["dest_asset"]

    from_label = a['from']
    to_label   = a['to']
    from_line  = f"From  {from_label}\n" if from_label else ""
    to_line    = f"To    {to_label}"     if to_label   else ""

    return (
        f"{intensity} Cross-Border Payment\n\n"
        f"{_fmt_number(a['amount'])} {src} → {dst}\n"
        f"Value: {_usd(usd)}\n\n"
        f"{from_line}"
        f"{to_line}"
    ).rstrip()


# ─── Soroban ──────────────────────────────────────────────────────────────────

def _soroban_liquidity(a):
    usd = a.get("amount_usd", 0)
    intensity = _intensity(usd).strip() if usd > 0 else "🤖"
    action = a.get("action", "event")

    return (
        f"{intensity} Soroban: Liquidity {action}\n\n"
        f"Protocol: {a['contract']}\n"
        f"Amount: {_usd(usd) if usd > 0 else 'N/A'}"
    )


def _soroban_swap(a):
    usd = a.get("amount_usd", 0)
    intensity = _intensity(usd).strip() if usd > 0 else "🤖"

    return (
        f"{intensity} Soroban: Swap Detected\n\n"
        f"Protocol: {a['contract']}\n"
        f"Volume: {_usd(usd) if usd > 0 else 'N/A'}"
    )


def _soroban_new_pool(a):
    return (
        f"🆕 New Soroban Liquidity Pool\n\n"
        f"Protocol: {a['contract']}"
    )


# ─── Meme Token Pump ──────────────────────────────────────────────────────────

def _meme_pump(a):
    code  = a["code"]
    pct   = a["pct_change"]
    tf    = a["timeframe"]
    price = a["price"]
    fdv   = a["fdv"]
    vol   = a["volume_24h"]

    if price < 0.000001:
        price_str = f"${price:.2e}"
    elif price < 0.01:
        price_str = f"${price:.6f}"
    elif price < 1:
        price_str = f"${price:.4f}"
    else:
        price_str = f"${price:.2f}"

    if pct >= 500:
        intensity = "🔥🔥🔥"
    elif pct >= 200:
        intensity = "🚀🚀"
    else:
        intensity = "🚀"

    lines = [
        f"{intensity} ${code} pumping on Stellar\n",
        f"{_pct(pct)} in {tf}",
        f"Price: {price_str}",
    ]
    if fdv > 0:
        lines.append(_fdv_str(fdv))
    if vol > 0:
        lines.append(f"24h volume: {_usd(vol)}")

    return "\n".join(lines)


# ─── SDEX Multi-pair ──────────────────────────────────────────────────────────

def _sdex_multi(a):
    usd   = a["usd_value"]
    intensity = _intensity(usd).strip()
    pairs = a.get("pairs", [])

    lines = ""
    for pair, pair_usd in pairs[:4]:
        lines += f"  {pair:<16} {_usd(pair_usd)}\n"

    return (
        f"{intensity} Multi-Pair DEX Activity\n\n"
        f"{lines}"
        f"Total volume: {_usd(usd)}"
    )


# ─── Airdrop / Mass Transfer ──────────────────────────────────────────────────

def _airdrop_summary(a):
    count     = a.get("count", 0)
    total     = a.get("total_usd", 0)
    per_tx    = a.get("per_tx_usd", 0)
    recipients = a.get("recipients", [])
    intensity = _intensity(total).strip()

    # Показываем первые 4 получателя в формате "Wallet 1 → Wallet 2"
    flow_lines = ""
    for i, r in enumerate(recipients[:4], 1):
        addr = r.get("address", "")
        short = f"Wallet {i}" if not addr else f"Wallet {i}"
        flow_lines += f"  → {short}\n"

    remaining = count - 4
    if remaining > 0:
        flow_lines += f"  → +{remaining} more wallets\n"

    return (
        f"{intensity} Mass Distribution\n\n"
        f"One sender → {count} wallets\n"
        f"{_usd(per_tx)} each  ·  {_usd(total)} total\n\n"
        f"{flow_lines}"
        f"Pattern: identical amounts, rapid succession"
    )


def format_airdrop_reply(a) -> str | None:
    """Reply со ссылками на все кошельки получателей."""
    recipients = a.get("recipients", [])
    if not recipients:
        return None

    lines = ["🔍 All recipient wallets:\n"]
    for i, r in enumerate(recipients[:10], 1):
        link = r.get("link", "")
        if link:
            lines.append(f"Wallet {i}: {link}")

    if len(recipients) > 10:
        lines.append(f"\n+{len(recipients) - 10} more wallets tracked")

    return "\n".join(lines)


# ─── Volume Spike ─────────────────────────────────────────────────────────────

def _volume_spike(a):
    pair  = a.get("pair", "?")
    usd   = a.get("usd_10min", 0)
    ratio = a.get("spike_ratio", 0)

    return (
        f"📈 Volume Spike: {pair}\n\n"
        f"{ratio:.1f}x above average\n"
        f"{_usd(usd)} traded in last 10 minutes"
    )




# ─── Telegram formatter (полный формат с адресами и ссылками) ────────────────

def format_telegram(alert: dict) -> str | None:
    """
    Telegram формат — старый стиль с эмодзи, парами, адресами и ссылками.
    """
    atype = alert.get("type", "")
    if atype == "WHALE_XLM":       return _tg_whale_xlm(alert)
    if atype == "WHALE_USDC":      return _tg_whale_usdc(alert)
    if atype == "SDEX":            return _tg_sdex(alert)
    if atype == "SDEX_MULTI":      return _tg_sdex_multi(alert)
    if atype == "CROSS_BORDER":    return _tg_cross_border(alert)
    if atype == "AIRDROP_SUMMARY": return _tg_airdrop(alert)
    return format_alert(alert)


STELLAR_EXPERT_ACC = "https://stellar.expert/explorer/public/account"


def _tg_addr_link(label, full_addr) -> str:
    """
    Возвращает кликабельную ссылку на адрес:
    - Известный: [Binance](ссылка)
    - Неизвестный: [GAVA...T4SR](ссылка)
    Без отдельной строки с полным адресом.
    """
    if not full_addr or len(full_addr) <= 8:
        return label or "Unknown"
    url = f"{STELLAR_EXPERT_ACC}/{full_addr}"
    display = label if label else f"{full_addr[:4]}...{full_addr[-4:]}"
    return f"[{display}]({url})"


# Оставляем для обратной совместимости
def _tg_addr(label, full_addr) -> str:
    return _tg_addr_link(label, full_addr)


def _tg_full(label, full_addr) -> str:
    """Больше не нужна — адрес теперь в ссылке."""
    return ""


def _tg_whale_xlm(a):
    usd       = a["usd_value"]
    inten     = _intensity(usd).strip()
    asset     = "XLM"
    from_str  = _tg_addr_link(a.get("from"), a.get("from_full", ""))
    to_str    = _tg_addr_link(a.get("to"),   a.get("to_full", ""))
    move_ctx  = _move_context(a.get("move_ctx"))
    ctx_line  = f"↳ {move_ctx}\n" if move_ctx else ""
    link      = a.get("link", "")
    return (
        f"{inten}\n"
        f"🐋 {_whale_label(usd)} — Stellar Network\n\n"
        f"{ctx_line}"
        f"📤 From: {from_str}\n"
        f"📥 To:   {to_str}\n"
        f"💰 {_fmt_number(a['amount'])} #{asset} ({_usd(usd)})\n\n"
        f"🔗 {link}"
    )


def _tg_whale_usdc(a):
    usd       = a["usd_value"]
    inten     = _intensity(usd).strip()
    asset     = a.get("asset", "USDC")
    from_str  = _tg_addr_link(a.get("from"), a.get("from_full", ""))
    to_str    = _tg_addr_link(a.get("to"),   a.get("to_full", ""))
    move_ctx  = _move_context(a.get("move_ctx"))
    ctx_line  = f"↳ {move_ctx}\n" if move_ctx else ""
    link      = a.get("link", "")
    return (
        f"{inten}\n"
        f"💵 {_whale_label(usd)} — #{asset} Transfer\n\n"
        f"{ctx_line}"
        f"📤 From: {from_str}\n"
        f"📥 To:   {to_str}\n"
        f"💰 {_fmt_number(a['amount'])} #{asset} ({_usd(usd)})\n\n"
        f"🔗 {link}"
    )


def _tg_sdex(a):
    usd   = a["usd_value"]
    inten = _intensity(usd).strip()
    is_mm = a.get("is_market_maker", False)
    mm    = " · Market Maker" if is_mm else ""
    link  = a.get("link","")
    return (
        f"{inten}\n"
        f"📊 SDEX Trade{mm} — Stellar DEX\n\n"
        f"🔄 Pair: {a.get('pair', a.get('selling','?')+'/'+a.get('buying','?'))}\n"
        f"📦 {_fmt_number(a.get('amount',0))} {a.get('selling','?')}\n"
        f"💰 Value: ~{_usd(usd)}\n\n"
        f"🔗 {link}"
    )


def _tg_sdex_multi(a):
    usd   = a["usd_value"]
    inten = _intensity(usd).strip()
    pairs = a.get("pairs", [])
    link  = a.get("link","")
    lines = ""
    for pair, pair_usd in pairs[:4]:
        lines += f"🔄 {pair:<16} ~{_usd(pair_usd)}\n"
    return (
        f"{inten}\n"
        f"📊 Multi-Pair TX — Stellar DEX\n\n"
        f"{lines}"
        f"💰 Total: ~{_usd(usd)}\n\n"
        f"🔗 {link}"
    )


def _tg_cross_border(a):
    usd      = a["usd_value"]
    inten    = _intensity(usd).strip()
    from_str = _tg_addr_link(a.get("from"), a.get("from_full", ""))
    to_str   = _tg_addr_link(a.get("to"),   a.get("to_full", ""))
    link     = a.get("link", "")
    return (
        f"{inten}\n"
        f"🌐 Cross-Border — Stellar\n\n"
        f"💸 {_fmt_number(a['amount'])} {a['src_asset']} → {a['dest_asset']}\n"
        f"💰 ~{_usd(usd)}\n"
        f"📤 {from_str}\n"
        f"📥 {to_str}\n\n"
        f"🔗 {link}"
    )


def _tg_airdrop(a):
    count      = a.get("count", 0)
    total      = a.get("total_usd", 0)
    per_tx     = a.get("per_tx_usd", 0)
    inten      = _intensity(total).strip()
    recipients = a.get("recipients", [])
    lines = ""
    for i, r in enumerate(recipients[:6], 1):
        lnk = r.get("link","")
        lines += f"  Wallet {i}: {lnk}\n" if lnk else f"  Wallet {i}\n"
    extra = f"  +{count-6} more\n" if count > 6 else ""
    return (
        f"{inten}\n"
        f"📦 Mass Distribution\n\n"
        f"🔁 {count} wallets · {_usd(per_tx)} each\n"
        f"💰 Total: {_usd(total)}\n\n"
        f"{lines}{extra}"
    )

# ─── Utils ────────────────────────────────────────────────────────────────────

def _trim(tweet: str, max_len: int = 280) -> str:
    if len(tweet) <= max_len:
        return tweet
    lines = tweet.split("\n")
    while len("\n".join(lines)) > max_len and lines:
        lines.pop()
    return "\n".join(lines)
