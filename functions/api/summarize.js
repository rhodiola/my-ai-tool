export async function onRequestPost(context) {
    const request = context.request
    const env = context.env

    let body
    try {
        body = await request.json()
    } catch {
        return Response.json({ error: "invalid json" }, { status: 400 })
    }

    const text = body.text
    const timestamp = body.timestamp

    if (!text || typeof text !== "string") {
        return Response.json({ error: "text is required" }, { status: 400 })
    }

    if (text.length > 300) {
        return Response.json({ error: "text too long (max 300 characters)" }, { status: 400 })
    }

    if (!timestamp || typeof timestamp !== "number") {
        return Response.json({ error: "timestamp is required" }, { status: 400 })
    }

    const now = Date.now()
    if (Math.abs(now - timestamp) > 300000) {
        return Response.json({ error: "expired request" }, { status: 403 })
    }

    const origin = request.headers.get("origin")
    const allowedOrigins = [
        "https://ai-sumco.com",
        "https://www.ai-sumco.com"
    ]

    if (origin && !allowedOrigins.includes(origin)) {
        return Response.json({ error: "forbidden origin" }, { status: 403 })
    }

    const ip = request.headers.get("CF-Connecting-IP") || "unknown"
    const rateKey = `rate_${ip}`

    let count = await env.RATE_LIMIT.get(rateKey)
    count = count ? parseInt(count, 10) : 0

    if (count >= 20) {
        return Response.json({ error: "rate limit exceeded" }, { status: 429 })
    }

    await env.RATE_LIMIT.put(rateKey, String(count + 1), { expirationTtl: 60 })

    const prompt = `Please summarize the following text clearly and concisely:\n\n${text}`

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
        return Response.json({ error: "invalid response from Gemini API" }, { status: 502 })
    }

    if (!geminiResponse.ok) {
        return Response.json(
            { error: data?.error?.message || "Gemini API error" },
            { status: geminiResponse.status }
        )
    }

    const summary = data?.candidates?.[0]?.content?.parts?.[0]?.text

    if (!summary) {
        return Response.json(
            { error: "no summary returned from Gemini" },
            { status: 502 }
        )
    }

    return Response.json({ summary })
}