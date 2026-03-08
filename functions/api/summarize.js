export async function onRequest(context) {
    const { request, env } = context;

    if (request.method !== "POST") {
        return new Response("Method not allowed", { status: 405 });
    }

    const body = await request.json();
    const textToSummarize = body.text;

    const response = await fetch("https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=" + env.GEMINI_API_KEY, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
            contents: [{ parts: [{ text: "以下の文章を簡潔に要約してください：\n\n" + textToSummarize }] }]
        })
    });

    const data = await response.json();
    const summary = data.candidates[0].content.parts[0].text;

    return new Response(JSON.stringify({ summary }), {
        headers: { "Content-Type": "application/json" }
    });
}