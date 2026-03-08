export async function onRequest(context) {
    const { request, env } = context;

    if (request.method !== "POST") {
        return new Response("Method Not Allowed", { status: 405 });
    }

    try {
        const body = await request.json();

        // 最新モデル gemini-2.5-flash を指定
        // もし404エラーが出る場合は、gemini-2.0-flash に書き換えてください
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
            // 400エラー（APIキー不正）などの詳細を画面に返す
            const errorMsg = data.error ? data.error.message : "不明なAPIエラー";
            return new Response(JSON.stringify({ summary: "AI通信エラー: " + errorMsg }), { status: response.status });
        }

        const summary = data.candidates[0].content.parts[0].text;
        return new Response(JSON.stringify({ summary }), {
            headers: { "Content-Type": "application/json" }
        });

    } catch (error) {
        return new Response(JSON.stringify({ summary: "システムエラー: " + error.message }), { status: 500 });
    }
}