export async function onRequestPost(context) {
    const { request, env } = context;

    try {
        const {
            baseHeader,
            baseSample,
            convertHeader,
            convertRows,
            comment
        } = await request.json();

        // APIキーはCloudflareの環境変数から取得
        const API_KEY = env.GEMINI_API_KEY;
        // モデルはご要望の gemini-3.1-flash-lite または gemma-3-12b-it を想定
        const MODEL_NAME = "gemma-3-27b-it";

        // Few-Shot プロンプトの構築
        const prompt = `
        あなたはデータ変換の専門家です。
        「入力データ」の各列を、「完成形フォーマット」のヘッダー順序に合わせて列単位で並べ替え、
        かつデータ形式を「完成形フォーマットのサンプル行」に合わせて整形してください。
 
        ### 1. 完成形フォーマット
        - ヘッダー: ${JSON.stringify(baseHeader)}
       - サンプル行（形式の参考。複数行）:
        ${baseSample.map(row => JSON.stringify(row)).join('\n')}

        ### 2. 変換用データ
        - ヘッダー: ${JSON.stringify(convertHeader)}
        - 入力データ（変換対象）:
        ${convertRows.map(row => JSON.stringify(row)).join('\n')}

        ### 3. 変換の厳守ルール
        - 「完成形フォーマット」の各ヘッダー項目に対し、入力データの中から意味的に最も適合する列を抽出し、指定の順序に並べ替えてください。
        - 1行だけで判断せず、複数行で共通する形式を優先すること
        - 日付、時間、電話番号、数値、ステータス表記などは、サンプル行の形式をテンプレートとし、項目単位で完全に統一してください。
        - 言語が違う場合は翻訳して揺らぎを持たせて関連性を調べて下さい。その際は英語表記を優先して下さい。
        - 入力データに対応する項目がない場合は空欄（""）としてください。
        - 全角/半角の統一、不要な空白の削除を自動で行ってください。
        - ユーザー指示: ${comment || "特になし"}

        ### 出力規定
        - 完成形フォーマットのヘッダー行を含めたCSV形式のみを出力してください。
        - 前置きや解説は一切含めず、純粋なCSVデータのみを返してください。
        - マークダウン記法（\`\`\`csv や \`\`\` など）は絶対に使用せず、プレーンなテキストデータとして直接出力してください。
        `;

        const response = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${MODEL_NAME}:generateContent?key=${API_KEY}`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                contents: [{ parts: [{ text: prompt }] }],
                generationConfig: {
                    temperature: 0.1,
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