export async function onRequestPost(context) {

    const request = context.request
    const env = context.env

    let body

    try {
        body = await request.json()
    } catch {
        return new Response("invalid json", { status: 400 })
    }

    const text = body.text
    const timestamp = body.timestamp


    /* ---------- 1 文字数制限 ---------- */

    if (!text || text.length > 300) {
        return new Response(
            JSON.stringify({ error: "text too long" }),
            { status: 400 }
        )
    }


    /* ---------- 2 Originチェック ---------- */

    const origin = request.headers.get("origin")

    if (origin !== "https://ai-sumco.com") {
        return new Response(
            JSON.stringify({ error: "forbidden origin" }),
            { status: 403 }
        )
    }


    /* ---------- 3 timestamp検証 ---------- */

    const now = Date.now()

    if (!timestamp || Math.abs(now - timestamp) > 300000) {
        return new Response(
            JSON.stringify({ error: "expired request" }),
            { status: 403 }
        )
    }


    /* ---------- 4 IPレート制限 ---------- */

    const ip = request.headers.get("CF-Connecting-IP")

    const key = `rate_${ip}`

    let count = await env.RATE_LIMIT.get(key)

    count = count ? parseInt(count) : 0

    if (count > 20) {
        return new Response(
            JSON.stringify({ error: "rate limit" }),
            { status: 429 }
        )
    }

    await env.RATE_LIMIT.put(
        key,
        count + 1,
        { expirationTtl: 60 }
    )


    /* ---------- 5 User-Agentチェック ---------- */

    const ua = request.headers.get("user-agent")

    if (!ua || ua.length < 10) {
        return new Response(
            JSON.stringify({ error: "invalid agent" }),
            { status: 403 }
        )
    }


    /* ---------- Gemini API ---------- */

    const geminiResponse = await fetch(
        "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=" + env.GEMINI_API_KEY,
        {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                contents: [
                    {
                        parts: [
                            {
                                text: `次の文章を要約してください:\n\n${text}`
                            }
                        ]
                    }
                ]
            })
        }
    )

    const data = await geminiResponse.json()

    const summary =
        data.candidates?.[0]?.content?.parts?.[0]?.text || "error"

    return new Response(
        JSON.stringify({ summary }),
        {
            headers: {
                "Content-Type": "application/json"
            }
        }
    )

}