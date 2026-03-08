export async function onRequest(context) {
    const { request, env } = context;

    if (request.method !== "POST") {
        return new Response("Method Not Allowed", { status: 405 });
    }

    try {
        const body = await request.json();

        // モデル名を gemini-2.5-flash に変更
        const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${env.GEMINI_API_KEY}`;

        const response = await fetch(url, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                contents: [{ parts: [{ text: "以下の文章を簡潔に要約してください：\n\n" + body.text }] }]
            })
        });

        const data = await response.json();

        if (!response.ok) {
            return new Response(JSON.stringify({ summary: "APIエラー: " + JSON.stringify(data) }), { status: response.status });
        }

        const summary = data.candidates[0].content.parts[0].text;
        return new Response(JSON.stringify({ summary }), {
            headers: { "Content-Type": "application/json" }
        });

    } catch (error) {
        return new Response(JSON.stringify({ summary: "システムエラー: " + error.message }), { status: 500 });
    }
}