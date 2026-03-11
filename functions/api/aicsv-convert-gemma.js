export async function onRequestPost(context) {

    const request = context.request
    const env = context.env
    const cache = caches.default

    /* ---------------------------
       Origin check
    --------------------------- */

    const allowedOrigins = [
        "https://ai.npaso.com"
    ]

    const origin = request.headers.get("Origin")

    if (!origin || !allowedOrigins.includes(origin)) {
        return Response.json(
            { error: "invalid origin" },
            { status: 403 }
        )
    }

    /* ---------------------------
       JSON取得
    --------------------------- */

    let body

    try {
        body = await request.json()
    } catch {
        return Response.json(
            { error: "invalid json" },
            { status: 400 }
        )
    }

    const baseHeader = body.baseHeader
    const baseSample = body.baseSample
    const convertHeader = body.convertHeader
    const convertSample = body.convertSample
    const convertRows = body.convertRows
    const comment = body.comment || ""
    const timestamp = body.timestamp

    /* ---------------------------
       timestamp check
    --------------------------- */

    const now = Date.now()

    if (!timestamp || Math.abs(now - timestamp) > 300000) {
        return Response.json(
            { error: "expired request" },
            { status: 403 }
        )
    }

    /* ---------------------------
       基本入力チェック
    --------------------------- */

    if (!Array.isArray(baseHeader) || !Array.isArray(convertHeader)) {
        return Response.json(
            { error: "invalid headers" },
            { status: 400 }
        )
    }

    if (!Array.isArray(baseSample) || !Array.isArray(convertSample) || !Array.isArray(convertRows)) {
        return Response.json(
            { error: "invalid rows" },
            { status: 400 }
        )
    }

    /* ---------------------------
       サイズ制限
    --------------------------- */

    if (comment.length > 300) {
        return Response.json(
            { error: "comment too long" },
            { status: 400 }
        )
    }

    if (baseHeader.length > 100 || convertHeader.length > 100) {
        return Response.json(
            { error: "too many columns" },
            { status: 400 }
        )
    }

    if (convertRows.length > 50000) {
        return Response.json(
            { error: "file too large" },
            { status: 400 }
        )
    }

    if (baseSample.length > 10 || convertSample.length > 10) {
        return Response.json(
            { error: "too many sample rows" },
            { status: 400 }
        )
    }

    /* ---------------------------
       Rate limit
    --------------------------- */

    const ip = request.headers.get("CF-Connecting-IP") || "unknown"
    const toolName = "aicsv-convert-gemma"
    const rateKey = `rate_${toolName}_${ip}`

    let count = await env.RATE_LIMIT.get(rateKey)
    count = count ? parseInt(count) : 0

    if (count >= 20) {
        return Response.json(
            { error: "rate limit exceeded" },
            { status: 429 }
        )
    }

    await env.RATE_LIMIT.put(
        rateKey,
        String(count + 1),
        { expirationTtl: 60 }
    )

    /* ---------------------------
       APIキー確認
    --------------------------- */

    if (!env.GEMMA_API_KEY) {
        return Response.json(
            { error: "missing GEMMA_API_KEY" },
            { status: 500 }
        )
    }

    /* ---------------------------
       キャッシュキー生成
    --------------------------- */

    const cacheSource = JSON.stringify({
        version: 2,
        model: "gemma-3-12b-it",
        baseHeader,
        baseSample,
        convertHeader,
        convertSample,
        comment
    })

    const cacheKey = await sha256Hex(cacheSource)
    const cacheRequest = new Request("https://cache/" + cacheKey)

    /* ---------------------------
       Workers Cache
    --------------------------- */

    let cacheResponse = await cache.match(cacheRequest)

    if (cacheResponse) {
        const cached = await cacheResponse.json()
        return processCSV(cached.mapping, cached.rules)
    }

    /* ---------------------------
       KV Cache
    --------------------------- */

    let kv = await env.AI_CACHE.get(cacheKey)

    if (kv) {
        const cached = JSON.parse(kv)

        await cache.put(
            cacheRequest,
            new Response(JSON.stringify(cached), {
                headers: {
                    "Content-Type": "application/json"
                }
            })
        )

        return processCSV(cached.mapping, cached.rules)
    }

    /* ---------------------------
       mapping × 2
    --------------------------- */

    const mappingPrompt1 = buildMappingPrompt({
        baseHeader,
        baseSample,
        convertHeader,
        convertSample,
        comment
    })

    const mappingPrompt2 = buildMappingPrompt({
        baseHeader,
        baseSample,
        convertHeader,
        convertSample,
        comment
    })

    let mapping1
    let mapping2

    try {
        [mapping1, mapping2] = await Promise.all([
            callGemmaGenerateContent(env, mappingPrompt1, "mapping"),
            callGemmaGenerateContent(env, mappingPrompt2, "mapping")
        ])
    } catch (error) {
        return Response.json(
            {
                error: "mapping generation failed",
                detail: String(error?.message || error)
            },
            { status: 502 }
        )
    }

    const mapping = mergeMappings(baseHeader, mapping1, mapping2, convertHeader)

    /* ---------------------------
       rules × 1
    --------------------------- */

    const rulesPrompt = buildRulesPrompt({
        baseHeader,
        baseSample,
        convertHeader,
        convertSample,
        mapping,
        comment
    })

    let rules

    try {
        rules = await callGemmaGenerateContent(env, rulesPrompt, "rules")
    } catch (error) {
        return Response.json(
            {
                error: "rules generation failed",
                detail: String(error?.message || error)
            },
            { status: 502 }
        )
    }

    const normalizedRules = normalizeRules(baseHeader, rules)

    /* ---------------------------
       キャッシュ保存
    --------------------------- */

    const cachePayload = {
        mapping,
        rules: normalizedRules
    }

    await env.AI_CACHE.put(
        cacheKey,
        JSON.stringify(cachePayload),
        { expirationTtl: 86400 }
    )

    await cache.put(
        cacheRequest,
        new Response(JSON.stringify(cachePayload), {
            headers: {
                "Content-Type": "application/json"
            }
        })
    )

    return processCSV(mapping, normalizedRules)

    /* ---------------------------
       CSV処理
    --------------------------- */

    function processCSV(mapping, rules) {

        const indexMap = baseHeader.map(col => {
            const source = mapping[col]
            const idx = convertHeader.indexOf(source)
            return idx
        })

        const result = []
        result.push(baseHeader)

        for (const row of convertRows) {

            const newRow = indexMap.map((sourceIndex, targetIndex) => {

                if (sourceIndex === -1) return ""

                const rawValue = row[sourceIndex] ?? ""
                const header = baseHeader[targetIndex]
                const rule = rules[header] || { actions: ["keep"] }

                return applyRule(rawValue, rule)
            })

            result.push(newRow)
        }

        const csv = result
            .map(r => r.map(escapeCSV).join(","))
            .join("\n")

        return Response.json({
            mapping,
            rules,
            rows: result.length - 1,
            csv
        })
    }
}

/* ---------------------------
   Prompt builders
--------------------------- */

function buildMappingPrompt({ baseHeader, baseSample, convertHeader, convertSample, comment }) {
    return `
You are a CSV column mapping engine.

Your task is ONLY to map Base CSV columns to Source CSV columns.

Priority rules (highest to lowest):
1. Follow USER INSTRUCTION strictly.
2. Use column name similarity.
3. Use sample value similarity.

USER INSTRUCTION (HIGHEST PRIORITY):
${comment || "(none)"}

Important rules:
- Always respect the user instruction.
- Use exact column names only.
- If the user instruction conflicts with inference, follow user instruction.
- If uncertain, return an empty string.
- Return JSON only.
- No explanation.

Base CSV header:
${JSON.stringify(baseHeader)}

Base CSV sample rows:
${JSON.stringify(baseSample)}

Source CSV header:
${JSON.stringify(convertHeader)}

Source CSV sample rows:
${JSON.stringify(convertSample)}

Return ONLY this JSON:
{
  "mapping": {
    "base_column_name": "source_column_name"
  }
}
`.trim()
}

function buildRulesPrompt({ baseHeader, baseSample, convertHeader, convertSample, mapping, comment }) {
    return `
You are a CSV normalization rule generator.

Your task:
Generate normalization rules for BASE columns.
The rules will be executed by program code later.

Priority rules (highest to lowest):
1. USER INSTRUCTION (must follow)
2. Column meaning
3. Sample value patterns

USER INSTRUCTION (HIGHEST PRIORITY):
${comment || "(none)"}

If the user specifies value conversion, create value_map rules.

Example:
"value_map": {
  "active": "有効",
  "inactive": "無効"
}

Allowed actions:
- "trim"
- "lowercase"
- "uppercase"
- "remove_spaces"
- "normalize_number"
- "normalize_integer"
- "normalize_date"
- "normalize_datetime"
- "normalize_phone"
- "normalize_postal"
- "keep"

Important rules:
- Use exact base column names only.
- Use conservative transformations.
- If uncertain, use ["keep"].
- Do not invent actions outside the allowed list.
- If the user instruction conflicts with inference, follow user instruction.
- Return JSON only.
- No explanation.

Base CSV header:
${JSON.stringify(baseHeader)}

Base CSV sample rows:
${JSON.stringify(baseSample)}

Source CSV header:
${JSON.stringify(convertHeader)}

Source CSV sample rows:
${JSON.stringify(convertSample)}

Confirmed mapping:
${JSON.stringify(mapping)}

Return ONLY this JSON:
{
  "rules": {
    "base_column_name": {
      "actions": ["trim", "normalize_date"],
      "value_map": {
        "active": "有効"
      }
    }
  }
}
`.trim()
}

/* ---------------------------
   Google AI Studio / generateContent
--------------------------- */

async function callGemmaGenerateContent(env, prompt, rootKey) {

    const url = "https://generativelanguage.googleapis.com/v1beta/models/gemma-3-12b-it:generateContent"

    const res = await fetch(url, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            "x-goog-api-key": env.GEMMA_API_KEY
        },
        body: JSON.stringify({
            contents: [
                {
                    parts: [
                        { text: prompt }
                    ]
                }
            ],
            generationConfig: {
                temperature: 0.2,
                maxOutputTokens: 700
            }
        })
    })

    let data

    try {
        data = await res.json()
    } catch {
        throw new Error("invalid gemma response")
    }

    if (!res.ok) {
        throw new Error(data?.error?.message || "gemma api error")
    }

    const text = extractGenerateContentText(data)
    const json = safeParseJson(text)

    if (!json || typeof json !== "object") {
        throw new Error("json parse failed")
    }

    if (!json[rootKey] || typeof json[rootKey] !== "object") {
        throw new Error(`${rootKey} missing`)
    }

    return json[rootKey]
}

function extractGenerateContentText(data) {
    const parts = data?.candidates?.[0]?.content?.parts || []
    return parts.map(p => p?.text || "").join("")
}

function safeParseJson(text) {

    if (!text) return null

    try {
        return JSON.parse(text)
    } catch {
        const match = text.match(/\{[\s\S]*\}/)
        if (!match) return null

        try {
            return JSON.parse(match[0])
        } catch {
            return null
        }
    }
}

/* ---------------------------
   Mapping merge
--------------------------- */

function mergeMappings(baseHeader, mapping1, mapping2, convertHeader) {

    const result = {}

    for (const col of baseHeader) {
        const v1 = normalizeMappedColumn(mapping1[col], convertHeader)
        const v2 = normalizeMappedColumn(mapping2[col], convertHeader)

        if (v1 && v2 && v1 === v2) {
            result[col] = v1
            continue
        }

        if (v1 && !v2) {
            result[col] = v1
            continue
        }

        if (!v1 && v2) {
            result[col] = v2
            continue
        }

        if (v1 && v2 && v1 !== v2) {
            result[col] = v1
            continue
        }

        result[col] = ""
    }

    return result
}

function normalizeMappedColumn(value, convertHeader) {
    if (!value || typeof value !== "string") return ""
    return convertHeader.includes(value) ? value : ""
}

/* ---------------------------
   Rules normalize
--------------------------- */

function normalizeRules(baseHeader, rules) {

    const allowed = new Set([
        "trim",
        "lowercase",
        "uppercase",
        "remove_spaces",
        "normalize_number",
        "normalize_integer",
        "normalize_date",
        "normalize_datetime",
        "normalize_phone",
        "normalize_postal",
        "keep"
    ])

    const result = {}

    for (const header of baseHeader) {
        const item = rules?.[header]
        const actions = Array.isArray(item?.actions) ? item.actions : ["keep"]
        const filtered = actions
            .filter(v => typeof v === "string" && allowed.has(v))
            .filter((v, i, arr) => arr.indexOf(v) === i)

        const valueMap = {}
        if (item?.value_map && typeof item.value_map === "object" && !Array.isArray(item.value_map)) {
            for (const [k, v] of Object.entries(item.value_map)) {
                valueMap[String(k)] = String(v)
            }
        }

        result[header] = {
            actions: filtered.length ? filtered : ["keep"],
            value_map: valueMap
        }
    }

    return result
}

/* ---------------------------
   Rule apply
--------------------------- */

function applyRule(value, rule) {

    if (value === null || value === undefined) return ""

    let out = String(value)

    if (rule?.value_map && typeof rule.value_map === "object") {
        if (Object.prototype.hasOwnProperty.call(rule.value_map, out)) {
            out = rule.value_map[out]
        }
    }

    const actions = Array.isArray(rule?.actions) ? rule.actions : ["keep"]

    for (const action of actions) {

        if (action === "keep") continue

        if (action === "trim") {
            out = out.trim()
            continue
        }

        if (action === "lowercase") {
            out = out.toLowerCase()
            continue
        }

        if (action === "uppercase") {
            out = out.toUpperCase()
            continue
        }

        if (action === "remove_spaces") {
            out = out.replace(/\s+/g, "")
            continue
        }

        if (action === "normalize_number") {
            out = normalizeNumber(out, false)
            continue
        }

        if (action === "normalize_integer") {
            out = normalizeNumber(out, true)
            continue
        }

        if (action === "normalize_date") {
            out = normalizeDateLike(out, false)
            continue
        }

        if (action === "normalize_datetime") {
            out = normalizeDateLike(out, true)
            continue
        }

        if (action === "normalize_phone") {
            out = out.replace(/[^\d+]/g, "")
            continue
        }

        if (action === "normalize_postal") {
            out = out.replace(/[^\d\-]/g, "")
            continue
        }
    }

    return out
}

function normalizeNumber(value, integerOnly) {

    let s = String(value).trim()

    if (!s) return s

    s = s
        .replace(/[￥¥円,$\s]/g, "")
        .replace(/,/g, "")

    if (!/^[-+]?\d+(\.\d+)?$/.test(s)) {
        return value
    }

    if (integerOnly) {
        return String(Math.trunc(Number(s)))
    }

    return String(Number(s))
}

function normalizeDateLike(value, withTime) {

    const parsed = parseDateTimeParts(value)
    if (!parsed) return value

    if (!withTime) {
        if (!parsed.year || !parsed.month || !parsed.day) return value
        return formatDate(parsed.year, parsed.month, parsed.day)
    }

    if (!parsed.year || !parsed.month || !parsed.day) return value

    return formatDateTime(
        parsed.year,
        parsed.month,
        parsed.day,
        parsed.hour ?? 0,
        parsed.minute ?? 0,
        parsed.second ?? 0
    )
}

/* ---------------------------
   Date parse helpers
--------------------------- */

function parseDateTimeParts(input) {

    if (input === null || input === undefined) return null

    const original = String(input).trim()
    if (!original) return null

    if (/^\d+(\.\d+)?$/.test(original)) {
        return null
    }

    let v = normalizeJapaneseDateText(original)
    let m

    m = v.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/)
    if (m) {
        const hh = Number(m[1])
        const mm = Number(m[2])
        const ss = Number(m[3] || 0)

        if (isValidTime(hh, mm, ss)) {
            return {
                type: "time",
                hour: hh,
                minute: mm,
                second: ss
            }
        }

        return null
    }

    m = v.match(/^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?$/)
    if (m) {
        const y = Number(m[1])
        const mo = Number(m[2])
        const d = Number(m[3])
        const hh = m[4] !== undefined ? Number(m[4]) : undefined
        const mm = m[5] !== undefined ? Number(m[5]) : undefined
        const ss = m[6] !== undefined ? Number(m[6]) : 0

        if (!isValidDate(y, mo, d)) return null

        return {
            type: hh !== undefined || mm !== undefined ? "datetime" : "date",
            year: y,
            month: mo,
            day: d,
            hour: hh,
            minute: mm,
            second: ss
        }
    }

    m = v.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?$/)
    if (m) {
        const a = Number(m[1])
        const b = Number(m[2])
        const y = Number(m[3])
        const hh = m[4] !== undefined ? Number(m[4]) : undefined
        const mm = m[5] !== undefined ? Number(m[5]) : undefined
        const ss = m[6] !== undefined ? Number(m[6]) : 0

        let mo
        let d

        if (a > 12 && b <= 12) {
            d = a
            mo = b
        } else if (b > 12 && a <= 12) {
            mo = a
            d = b
        } else {
            return null
        }

        if (!isValidDate(y, mo, d)) return null

        return {
            type: hh !== undefined || mm !== undefined ? "datetime" : "date",
            year: y,
            month: mo,
            day: d,
            hour: hh,
            minute: mm,
            second: ss
        }
    }

    return null
}

function normalizeJapaneseDateText(value) {
    return String(value)
        .trim()
        .replace(/[　]/g, " ")
        .replace(/年/g, "-")
        .replace(/月/g, "-")
        .replace(/日/g, "")
        .replace(/時/g, ":")
        .replace(/分/g, ":")
        .replace(/秒/g, "")
        .replace(/\s+/g, " ")
        .replace(/T/g, " ")
        .replace(/Z$/i, "")
}

function isValidDate(year, month, day) {
    if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) return false
    if (month < 1 || month > 12) return false
    if (day < 1 || day > 31) return false

    const dt = new Date(year, month - 1, day)

    return (
        dt.getFullYear() === year &&
        dt.getMonth() === month - 1 &&
        dt.getDate() === day
    )
}

function isValidTime(hour, minute, second) {
    return (
        (hour === undefined || Number.isInteger(hour)) &&
        (minute === undefined || Number.isInteger(minute)) &&
        (second === undefined || Number.isInteger(second)) &&
        (hour ?? 0) >= 0 && (hour ?? 0) <= 23 &&
        (minute ?? 0) >= 0 && (minute ?? 0) <= 59 &&
        (second ?? 0) >= 0 && (second ?? 0) <= 59
    )
}

function pad2(n) {
    return String(n).padStart(2, "0")
}

function formatDate(year, month, day) {
    return `${year}-${pad2(month)}-${pad2(day)}`
}

function formatTime(hour, minute, second) {
    return `${pad2(hour)}:${pad2(minute)}:${pad2(second)}`
}

function formatDateTime(year, month, day, hour, minute, second) {
    return `${formatDate(year, month, day)} ${formatTime(hour, minute, second)}`
}

/* ---------------------------
   CSV helpers
--------------------------- */

function escapeCSV(value) {

    if (value === null || value === undefined) return ""

    const str = String(value)

    if (str.includes('"')) {
        return `"${str.replace(/"/g, '""')}"`
    }

    if (str.includes(",") || str.includes("\n")) {
        return `"${str}"`
    }

    return str
}

/* ---------------------------
   Hash helper
--------------------------- */

async function sha256Hex(text) {
    const encoder = new TextEncoder()
    const data = encoder.encode(text)
    const digest = await crypto.subtle.digest("SHA-256", data)
    const hashArray = Array.from(new Uint8Array(digest))
    return hashArray.map(b => b.toString(16).padStart(2, "0")).join("")
}