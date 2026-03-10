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

    if (!Array.isArray(convertRows)) {
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


    /* ---------------------------
       Rate limit
    --------------------------- */

    const ip = request.headers.get("CF-Connecting-IP") || "unknown"
    const key = `rate_${ip}`

    let count = await env.RATE_LIMIT.get(key)

    count = count ? parseInt(count) : 0

    if (count >= 20) {
        return Response.json(
            { error: "rate limit exceeded" },
            { status: 429 }
        )
    }

    await env.RATE_LIMIT.put(
        key,
        count + 1,
        { expirationTtl: 60 }
    )


    /* ---------------------------
       Gemini prompt
    --------------------------- */

    const prompt = `
You analyze CSV structures.

Base CSV header:
${JSON.stringify(baseHeader)}

Base CSV sample rows:
${JSON.stringify(baseSample)}

Source CSV header:
${JSON.stringify(convertHeader)}

Source CSV sample rows:
${JSON.stringify(convertSample)}

User note:
${comment}

Return ONLY JSON.

Example:
{
 "mapping": {
   "base_column": "source_column"
 }
}
`


    /* ---------------------------
       キャッシュキー生成
    --------------------------- */

    const hashSource = JSON.stringify({
        baseHeader,
        baseSample,
        convertHeader,
        convertSample,
        comment
    })

    const encoder = new TextEncoder()
    const data = encoder.encode(hashSource)

    const digest = await crypto.subtle.digest("SHA-256", data)
    const hashArray = Array.from(new Uint8Array(digest))
    const cacheKey = hashArray.map(b => b.toString(16).padStart(2, "0")).join("")


    /* ---------------------------
       Workers Cache
    --------------------------- */

    const cacheRequest = new Request("https://cache/" + cacheKey)

    let cacheResponse = await cache.match(cacheRequest)

    if (cacheResponse) {
        const mapping = await cacheResponse.json()
        return processCSV(mapping)
    }


    /* ---------------------------
       KV Cache
    --------------------------- */

    let kv = await env.AI_CACHE.get(cacheKey)

    if (kv) {
        const mapping = JSON.parse(kv)

        await cache.put(
            cacheRequest,
            new Response(JSON.stringify(mapping))
        )

        return processCSV(mapping)
    }


    /* ---------------------------
       Gemini API
    --------------------------- */

    const geminiResponse = await fetch(
        `https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-flash-lite-preview:generateContent?key=${env.GEMINI_API_KEY}`,
        {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
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
                    maxOutputTokens: 1024,
                    temperature: 0.1
                }
            })
        }
    )

    let dataRes

    try {
        dataRes = await geminiResponse.json()
    } catch {
        return Response.json(
            { error: "invalid Gemini response" },
            { status: 502 }
        )
    }

    if (!geminiResponse.ok) {
        return Response.json(
            { error: dataRes?.error?.message || "Gemini API error" },
            { status: geminiResponse.status }
        )
    }


    /* ---------------------------
       mapping取得
    --------------------------- */

    const parts = dataRes?.candidates?.[0]?.content?.parts || []
    const text = parts.map(p => p.text || "").join("")

    let mapping

    try {
        const parsed = JSON.parse(text)
        mapping = parsed.mapping

        if (!mapping || typeof mapping !== "object") {
            throw new Error()
        }

    } catch {
        return Response.json(
            { error: "mapping parse failed", raw: text },
            { status: 502 }
        )
    }


    /* ---------------------------
       キャッシュ保存
    --------------------------- */

    await env.AI_CACHE.put(
        cacheKey,
        JSON.stringify(mapping),
        { expirationTtl: 86400 }
    )

    await cache.put(
        cacheRequest,
        new Response(JSON.stringify(mapping))
    )

    return processCSV(mapping)



    /* ---------------------------
       CSV処理
    --------------------------- */

    function processCSV(mapping) {

        const indexMap = baseHeader.map(col => {
            const source = mapping[col]
            const idx = convertHeader.indexOf(source)
            return idx
        })

        const columnSpecs = baseHeader.map((header, targetIndex) => {
            const samples = baseSample
                .map(row => row?.[targetIndex])
                .filter(v => v !== undefined && v !== null && String(v).trim() !== "")
            return detectTargetSpec(header, samples)
        })

        const result = []

        result.push(baseHeader)

        for (const row of convertRows) {

            const newRow = indexMap.map((sourceIndex, targetIndex) => {

                if (sourceIndex === -1) return ""

                const value = row[sourceIndex] ?? ""
                const spec = columnSpecs[targetIndex]

                return normalizeBySpec(value, spec)

            })

            result.push(newRow)

        }

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

        const csv = result
            .map(r => r.map(escapeCSV).join(","))
            .join("\n")

        return Response.json({
            mapping,
            rows: result.length - 1,
            csv
        })
    }



    /* ---------------------------
       型推定
    --------------------------- */

    function detectTargetSpec(header, samples) {

        const headerText = String(header || "").toLowerCase()

        const looksLikeDateHeader =
            /date|day|birthday|dob|created|updated|shipped|ordered|delivery|time|datetime|timestamp|日時|日付|時刻|年月日|作成日|更新日|発送日|注文日/.test(headerText)

        if (!samples.length) {
            return { type: looksLikeDateHeader ? "dateish" : "text" }
        }

        let dateCount = 0
        let datetimeCount = 0
        let timeCount = 0

        for (const raw of samples) {
            const kind = detectValueKind(raw)

            if (kind === "datetime") datetimeCount++
            else if (kind === "date") dateCount++
            else if (kind === "time") timeCount++
        }

        const total = samples.length
        const best = Math.max(dateCount, datetimeCount, timeCount)

        if (best === 0) {
            return { type: looksLikeDateHeader ? "dateish" : "text" }
        }

        if (!looksLikeDateHeader && best < Math.ceil(total * 0.6)) {
            return { type: "text" }
        }

        if (datetimeCount >= dateCount && datetimeCount >= timeCount) {
            return { type: "datetime" }
        }

        if (timeCount >= dateCount && timeCount >= datetimeCount) {
            return { type: "time" }
        }

        return { type: "date" }
    }


    function detectValueKind(value) {
        const parsed = parseDateTimeParts(value)
        return parsed ? parsed.type : null
    }


    /* ---------------------------
       値変換
    --------------------------- */

    function normalizeBySpec(value, spec) {

        if (value === null || value === undefined) return ""
        if (!spec || spec.type === "text") return value

        const parsed = parseDateTimeParts(value)

        if (!parsed) return value

        if (spec.type === "date") {
            if (!parsed.year || !parsed.month || !parsed.day) return value
            return formatDate(parsed.year, parsed.month, parsed.day)
        }

        if (spec.type === "datetime") {
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

        if (spec.type === "time") {
            if (parsed.hour === undefined || parsed.minute === undefined) return value
            return formatTime(
                parsed.hour,
                parsed.minute,
                parsed.second ?? 0
            )
        }

        if (spec.type === "dateish") {
            if (parsed.type === "datetime") {
                return formatDateTime(
                    parsed.year,
                    parsed.month,
                    parsed.day,
                    parsed.hour ?? 0,
                    parsed.minute ?? 0,
                    parsed.second ?? 0
                )
            }

            if (parsed.type === "date") {
                return formatDate(parsed.year, parsed.month, parsed.day)
            }

            if (parsed.type === "time") {
                return formatTime(
                    parsed.hour ?? 0,
                    parsed.minute ?? 0,
                    parsed.second ?? 0
                )
            }
        }

        return value
    }


    /* ---------------------------
       厳格パーサ
       数字だけは絶対に日付化しない
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

        /* Time only: HH:mm or HH:mm:ss */
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

        /* YYYY-MM-DD or YYYY/MM/DD with optional time */
        m = v.match(
            /^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?$/
        )
        if (m) {
            const y = Number(m[1])
            const mo = Number(m[2])
            const d = Number(m[3])
            const hh = m[4] !== undefined ? Number(m[4]) : undefined
            const mm = m[5] !== undefined ? Number(m[5]) : undefined
            const ss = m[6] !== undefined ? Number(m[6]) : 0

            if (!isValidDate(y, mo, d)) return null

            if (hh !== undefined || mm !== undefined) {
                if (!isValidTime(hh ?? 0, mm ?? 0, ss ?? 0)) return null
                return {
                    type: "datetime",
                    year: y,
                    month: mo,
                    day: d,
                    hour: hh ?? 0,
                    minute: mm ?? 0,
                    second: ss ?? 0
                }
            }

            return {
                type: "date",
                year: y,
                month: mo,
                day: d
            }
        }

        /* DD/MM/YYYY or MM/DD/YYYY only when unambiguous */
        m = v.match(
            /^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?$/
        )
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

            if (hh !== undefined || mm !== undefined) {
                if (!isValidTime(hh ?? 0, mm ?? 0, ss ?? 0)) return null
                return {
                    type: "datetime",
                    year: y,
                    month: mo,
                    day: d,
                    hour: hh ?? 0,
                    minute: mm ?? 0,
                    second: ss ?? 0
                }
            }

            return {
                type: "date",
                year: y,
                month: mo,
                day: d
            }
        }

        /* English month names */
        const monthMap = {
            jan: 1, january: 1,
            feb: 2, february: 2,
            mar: 3, march: 3,
            apr: 4, april: 4,
            may: 5,
            jun: 6, june: 6,
            jul: 7, july: 7,
            aug: 8, august: 8,
            sep: 9, sept: 9, september: 9,
            oct: 10, october: 10,
            nov: 11, november: 11,
            dec: 12, december: 12
        }

        m = v.match(
            /^([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/
        )
        if (m) {
            const mo = monthMap[m[1].toLowerCase()]
            const d = Number(m[2])
            const y = Number(m[3])
            const hh = m[4] !== undefined ? Number(m[4]) : undefined
            const mm = m[5] !== undefined ? Number(m[5]) : undefined
            const ss = m[6] !== undefined ? Number(m[6]) : 0

            if (!mo || !isValidDate(y, mo, d)) return null

            if (hh !== undefined || mm !== undefined) {
                if (!isValidTime(hh ?? 0, mm ?? 0, ss ?? 0)) return null
                return {
                    type: "datetime",
                    year: y,
                    month: mo,
                    day: d,
                    hour: hh ?? 0,
                    minute: mm ?? 0,
                    second: ss ?? 0
                }
            }

            return {
                type: "date",
                year: y,
                month: mo,
                day: d
            }
        }

        m = v.match(
            /^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/
        )
        if (m) {
            const d = Number(m[1])
            const mo = monthMap[m[2].toLowerCase()]
            const y = Number(m[3])
            const hh = m[4] !== undefined ? Number(m[4]) : undefined
            const mm = m[5] !== undefined ? Number(m[5]) : undefined
            const ss = m[6] !== undefined ? Number(m[6]) : 0

            if (!mo || !isValidDate(y, mo, d)) return null

            if (hh !== undefined || mm !== undefined) {
                if (!isValidTime(hh ?? 0, mm ?? 0, ss ?? 0)) return null
                return {
                    type: "datetime",
                    year: y,
                    month: mo,
                    day: d,
                    hour: hh ?? 0,
                    minute: mm ?? 0,
                    second: ss ?? 0
                }
            }

            return {
                type: "date",
                year: y,
                month: mo,
                day: d
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
            Number.isInteger(hour) &&
            Number.isInteger(minute) &&
            Number.isInteger(second) &&
            hour >= 0 && hour <= 23 &&
            minute >= 0 && minute <= 59 &&
            second >= 0 && second <= 59
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

}