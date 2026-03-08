export async function onRequestPost(context) {

    const { text } = await context.request.json()

    if (!text) {
        return Response.json({ summary: "No text provided" })
    }

    if (text.length > 300) {
        return Response.json({
            summary: "文字数が多すぎます（最大300文字）"
        })
    }

    const apiKey = context.env.GEMINI_API_KEY

    const response = await fetch(
        "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=" + apiKey,
        {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                contents: [{
                    parts: [{
                        text: "次の文章を要約してください:\n" + text
                    }]
                }]
            })
        }
    )

    const data = await response.json()

    const summary =
        data.candidates?.[0]?.content?.parts?.[0]?.text ?? ""

    return Response.json({ summary })
}