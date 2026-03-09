export async function onRequestPost(context) {

    const request = context.request
    const env = context.env

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

    const text = body.text
    const timestamp = body.timestamp


    /* ---------------------------
       入力チェック
    --------------------------- */

    if (!text || typeof text !== "string") {
        return Response.json(
            { error: "text is required" },
            { status: 400 }
        )
    }

    if (text.length > 300) {
        return Response.json(
            { error: "text too long (max 300 characters)" },
            { status: 400 }
        )
    }

    if (!timestamp || typeof timestamp !== "number") {
        return Response.json(
            { error: "timestamp missing" },
            { status: 400 }
        )
    }

    const now = Date.now()

    if (Math.abs(now - timestamp) > 300000) {
        return Response.json(
            { error: "expired request" },
            { status: 403 }
        )
    }


    /* ---------------------------
       IPレート制限
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
       Gemini API 呼び出し
    --------------------------- */

    const prompt =
        `Please summarize the following text clearly and concisely:

${text}`

    const geminiResponse = await fetch(
        `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${env.GEMINI_API_KEY}`,
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
                    maxOutputTokens: 200
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
       レスポンス整形
    --------------------------- */

    const parts = data?.candidates?.[0]?.content?.parts || []

    const summary = parts
        .map(p => p.text || "")
        .join("")

    if (!summary) {
        return Response.json(
            { error: "no summary returned" },
            { status: 502 }
        )
    }

    return Response.json({ summary })

}