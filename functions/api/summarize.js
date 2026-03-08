export async function onRequest(context) {
    const { request, env } = context;

    // 1. 本文を取得
    const body = await request.json();

    // 2. Geminiへリクエスト
    const response = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-flash-lite-preview:generateContent?key=${env.GEMINI_API_KEY}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
            contents: [{ parts: [{ text: `以下の文章を簡潔に要約してください：\n\n${body.text}` }] }]
        })
    });

    // 3. エラーハンドリングを追加
    if (!response.ok) {
        return new Response(JSON.stringify({ summary: "AIとの通信エラー: " + response.status }), { status: response.status });
    }

    const data = await response.json();
    const summary = data.candidates[0].content.parts[0].text;

    return new Response(JSON.stringify({ summary }), {
        headers: { "Content-Type": "application/json" }
    });
}