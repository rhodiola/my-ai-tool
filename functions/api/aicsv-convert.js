export async function onRequestPost(context) {

    const request = context.request
    const env = context.env


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
       Gemini API
    --------------------------- */

    const geminiResponse = await fetch(
        `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-lite-001:generateContent?key=${env.GEMINI_API_KEY}`,
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


    let data

    try {
        data = await geminiResponse.json()
    } catch {
        return Response.json(
            { error: "invalid Gemini response" },
            { status: 502 }
        )
    }

    if (!geminiResponse.ok) {
        return Response.json(
            { error: data?.error?.message || "Gemini API error" },
            { status: geminiResponse.status }
        )
    }


    /* ---------------------------
       mapping取得
    --------------------------- */

    const parts = data?.candidates?.[0]?.content?.parts || []

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
       CSV変換
    --------------------------- */

    const indexMap = baseHeader.map(col => {

        const source = mapping[col]

        const idx = convertHeader.indexOf(source)

        return idx

    })

    const result = []

    result.push(baseHeader)

    for (const row of convertRows) {

        const newRow = indexMap.map(i => {

            if (i === -1) return ""

            return row[i] || ""

        })

        result.push(newRow)

    }


    /* ---------------------------
       CSV生成
    --------------------------- */

    const csv = result
        .map(r => r.join(","))
        .join("\n")


    /* ---------------------------
       return
    --------------------------- */

    return Response.json({
        mapping,
        rows: result.length - 1,
        csv
    })

}