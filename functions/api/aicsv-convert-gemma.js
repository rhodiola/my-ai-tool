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
        return Response.json({ error: "invalid origin" }, { status: 403 })
    }

    /* ---------------------------
    JSON取得
    --------------------------- */

    let body

    try {
        body = await request.json()
    } catch {
        return Response.json({ error: "invalid json" }, { status: 400 })
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
        return Response.json({ error: "expired request" }, { status: 403 })
    }

    /* ---------------------------
    basic validation
    --------------------------- */

    if (!Array.isArray(baseHeader) || !Array.isArray(convertHeader)) {
        return Response.json({ error: "invalid headers" }, { status: 400 })
    }

    if (!Array.isArray(baseSample) || !Array.isArray(convertSample) || !Array.isArray(convertRows)) {
        return Response.json({ error: "invalid rows" }, { status: 400 })
    }

    /* ---------------------------
    size limits
    --------------------------- */

    if (comment.length > 300) {
        return Response.json({ error: "comment too long" }, { status: 400 })
    }

    if (baseHeader.length > 100 || convertHeader.length > 100) {
        return Response.json({ error: "too many columns" }, { status: 400 })
    }

    if (convertRows.length > 50000) {
        return Response.json({ error: "file too large" }, { status: 400 })
    }

    if (baseSample.length > 10 || convertSample.length > 10) {
        return Response.json({ error: "too many sample rows" }, { status: 400 })
    }

    /* ---------------------------
    rate limit
    --------------------------- */

    const ip = request.headers.get("CF-Connecting-IP") || "unknown"
    const rateKey = `rate_gemma_${ip}`

    let count = await env.RATE_LIMIT.get(rateKey)
    count = count ? parseInt(count) : 0

    if (count >= 20) {
        return Response.json({ error: "rate limit exceeded" }, { status: 429 })
    }

    await env.RATE_LIMIT.put(rateKey, String(count + 1), { expirationTtl: 60 })

    /* ---------------------------
    API key
    --------------------------- */

    if (!env.GEMMA_API_KEY) {
        return Response.json({ error: "missing GEMMA_API_KEY" }, { status: 500 })
    }

    /* ---------------------------
    cache key
    --------------------------- */

    const cacheSource = JSON.stringify({
        version: 4,
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
    Workers cache
    --------------------------- */

    let cacheResponse = await cache.match(cacheRequest)

    if (cacheResponse) {
        const cached = await cacheResponse.json()
        return processCSV(cached.mapping, cached.rules)
    }

    /* ---------------------------
    KV cache
    --------------------------- */

    let kv = await env.AI_CACHE.get(cacheKey)

    if (kv) {

        const cached = JSON.parse(kv)

        await cache.put(
            cacheRequest,
            new Response(JSON.stringify(cached), {
                headers: { "Content-Type": "application/json" }
            })
        )

        return processCSV(cached.mapping, cached.rules)

    }

    /* ---------------------------
    mapping ×2
    --------------------------- */

    const mappingPrompt = buildMappingPrompt({
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
            callGemmaGenerateContent(env, mappingPrompt, "mapping"),
            callGemmaGenerateContent(env, mappingPrompt, "mapping")
        ])

    } catch (error) {

        return Response.json({
            error: "mapping generation failed",
            detail: String(error?.message || error)
        }, { status: 502 })

    }

    let mapping = mergeMappings(baseHeader, mapping1, mapping2, convertHeader)
    mapping = improveMapping(mapping, baseHeader, convertHeader)

    /* ---------------------------
    rules ×1
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

        return Response.json({
            error: "rules generation failed",
            detail: String(error?.message || error)
        }, { status: 502 })

    }

    const normalizedRules = normalizeRules(baseHeader, rules)

    /* ---------------------------
    cache save
    --------------------------- */

    const payload = { mapping, rules: normalizedRules }

    await env.AI_CACHE.put(cacheKey, JSON.stringify(payload), { expirationTtl: 86400 })

    await cache.put(cacheRequest, new Response(JSON.stringify(payload), {
        headers: { "Content-Type": "application/json" }
    }))

    return processCSV(mapping, normalizedRules)

    /* ---------------------------
    CSV処理
    --------------------------- */

    function processCSV(mapping, rules) {

        const indexMap = baseHeader.map(col => {
            const source = mapping[col]
            return convertHeader.indexOf(source)
        })

        const result = []
        result.push(baseHeader)

        for (const row of convertRows) {

            const newRow = indexMap.map((idx, targetIndex) => {

                if (idx === -1) return ""

                const rawValue = row[idx] ?? ""
                const header = baseHeader[targetIndex]

                const rule = rules[header] || { actions: ["keep"], value_map: {} }

                return applyRule(rawValue, rule)

            })

            result.push(newRow)

        }

        const csv = result.map(r => r.map(escapeCSV).join(",")).join("\n")

        return Response.json({
            mapping,
            rules,
            rows: result.length - 1,
            csv
        })

    }

}

/* ---------------------------
Mapping改善
--------------------------- */

function improveMapping(mapping, baseHeader, convertHeader) {

    const result = { ...mapping }

    for (const base of baseHeader) {

        if (result[base]) continue

        const baseNorm = normalize(base)

        /* id専用マッピング */

        if (baseNorm === "id") {

            for (const src of convertHeader) {

                const srcNorm = normalize(src)

                if (srcNorm === "id" || srcNorm.endsWith("id")) {
                    result[base] = src
                    break
                }

            }

            continue
        }

        /* normalized一致 */

        for (const src of convertHeader) {

            if (normalize(src) === baseNorm) {
                result[base] = src
                break
            }

        }

    }

    return result

}

/* ---------------------------
Prompt builders
--------------------------- */

function buildMappingPrompt({ baseHeader, baseSample, convertHeader, convertSample, comment }) {

    return `
You are a CSV column mapping engine.

USER INSTRUCTION:
${comment || "(none)"}

Base CSV header:
${JSON.stringify(baseHeader)}

Base sample:
${JSON.stringify(baseSample)}

Source header:
${JSON.stringify(convertHeader)}

Source sample:
${JSON.stringify(convertSample)}

Return JSON:

{
"mapping":{
"base_column":"source_column"
}
}
`.trim()

}

function buildRulesPrompt({ baseHeader, baseSample, convertHeader, convertSample, mapping, comment }) {

    return `
Generate normalization rules.

USER INSTRUCTION:
${comment || "(none)"}

Base header:
${JSON.stringify(baseHeader)}

Mapping:
${JSON.stringify(mapping)}

Return JSON:

{
"rules":{
"column":{
"actions":["keep"],
"value_map":{}
}
}
}
`.trim()

}

/* ---------------------------
Gemma API
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
                    parts: [{ text: prompt }]
                }
            ],
            generationConfig: {
                temperature: 0.2,
                maxOutputTokens: 700
            }
        })
    })

    const data = await res.json()

    if (!res.ok) {
        throw new Error(data?.error?.message || "gemma error")
    }

    const parts = data?.candidates?.[0]?.content?.parts || []
    const text = parts.map(p => p?.text || "").join("")

    const json = safeParseJson(text)

    if (!json || !json[rootKey]) {
        throw new Error("json parse failed")
    }

    return json[rootKey]

}

function safeParseJson(text) {

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
merge
--------------------------- */

function mergeMappings(baseHeader, m1, m2, convertHeader) {

    const result = {}

    for (const col of baseHeader) {

        const v1 = convertHeader.includes(m1[col]) ? m1[col] : ""
        const v2 = convertHeader.includes(m2[col]) ? m2[col] : ""

        if (v1 && v1 === v2) {
            result[col] = v1
            continue
        }

        if (v1) result[col] = v1
        else if (v2) result[col] = v2
        else result[col] = ""

    }

    return result

}

/* ---------------------------
rules normalize
--------------------------- */

function normalizeRules(baseHeader, rules) {

    const result = {}

    for (const h of baseHeader) {

        const item = rules?.[h] || {}

        const actions = Array.isArray(item.actions) ? item.actions : ["keep"]
        const valueMap = typeof item.value_map === "object" ? item.value_map : {}

        result[h] = {
            actions,
            value_map: valueMap
        }

    }

    return result

}

/* ---------------------------
rule apply
--------------------------- */

function applyRule(value, rule) {

    let out = String(value ?? "")

    if (rule.value_map && rule.value_map[out]) {
        out = rule.value_map[out]
    }

    for (const action of rule.actions) {

        if (action === "trim") out = out.trim()
        if (action === "lowercase") out = out.toLowerCase()
        if (action === "uppercase") out = out.toUpperCase()

    }

    return out

}

/* ---------------------------
helpers
--------------------------- */

function normalize(s) {
    return String(s).toLowerCase().replace(/[_\- ]/g, "")
}

function escapeCSV(v) {

    if (v == null) return ""

    const s = String(v)

    if (s.includes('"')) return `"${s.replace(/"/g, '""')}"`

    if (s.includes(",") || s.includes("\n")) return `"${s}"`

    return s

}

async function sha256Hex(text) {

    const encoder = new TextEncoder()
    const data = encoder.encode(text)

    const hash = await crypto.subtle.digest("SHA-256", data)

    return [...new Uint8Array(hash)]
        .map(b => b.toString(16).padStart(2, "0"))
        .join("")

}