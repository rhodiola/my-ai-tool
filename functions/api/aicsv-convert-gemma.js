export async function onRequestPost(context) {
    const { request, env } = context;

    try {
        const {
            baseHeader,
            baseSample,
            convertHeader,
            convertSample,
            convertRows,
            comment
        } = await request.json();

        // APIキーはCloudflareの環境変数から取得
        const API_KEY = env.GEMINI_API_KEY;
        // モデルはご要望の gemini-3.1-flash-lite または gemma-3-12b-it を想定
        const MODEL_NAME = "gemini-3.1-flash-lite-preview";

        // Few-Shot プロンプトの構築
        const prompt = `
あなたはデータ変換とフォーマット整形の専門家です。
「変換用CSV」の各行を解析し、「完成形CSV」の列構成とデータ形式に厳密に合わせて変換してください。

### 1. 完成形CSV（出力の目標）
- ヘッダー: ${JSON.stringify(baseHeader)}
- サンプルデータ（この形式に似せてください）:
${baseSample.map(row => JSON.stringify(row)).join('\n')}

### 2. 変換用CSV（現在のデータ構造）
- ヘッダー: ${JSON.stringify(convertHeader)}
- サンプルデータ（変換前）:
${convertSample.map(row => JSON.stringify(row)).join('\n')}

### 3. ユーザーからの個別指示
${comment || "特になし"}

### 変換の厳守ルール
- 「完成形CSV」のヘッダー順に従って列を配置してください。
- 日付・数値・時間の表記、および言語（翻訳の要否）は、完成形CSVのサンプルデータに100%合わせてください。
- 入力データに対応する列がない場合は空文字（""）にしてください。
- 出力は **CSVのデータ行のみ** とし、ヘッダー、解説、挨拶は一切含めないでください。
- 各値はダブルクォートで囲み、カンマ区切りにしてください。

### 変換対象データ（これを変換してください）
${convertRows.map(row => JSON.stringify(row)).join('\n')}
`;

        const response = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${MODEL_NAME}:generateContent?key=${API_KEY}`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                contents: [{ parts: [{ text: prompt }] }],
                generationConfig: {
                    temperature: 0.1, // 一貫性を高めるために低めに設定
                }
            })
        });

        const data = await response.json();

        if (data.error) {
            return new Response(JSON.stringify({ error: data.error.message }), {
                status: 500,
                headers: { "Content-Type": "application/json" }
            });
        }

        const resultText = data.candidates[0].content.parts[0].text.trim();

        // フロントエンドが期待する { csv: "..." } 形式で返却
        return new Response(JSON.stringify({ csv: resultText }), {
            headers: { "Content-Type": "application/json" }
        });

    } catch (error) {
        return new Response(JSON.stringify({ error: error.message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
        });
    }
}