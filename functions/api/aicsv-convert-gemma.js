export async function onRequestPost(context) {
    const { request, env } = context;

    try {
        const {
            baseHeader,
            baseSample,
            convertHeader,
            convertRows,
            comment,
            headerMissing
        } = await request.json();

        const API_KEY = env.GEMINI_API_KEY;
        const MODEL_NAME = "gemma-3-27b-it";

        const prompt = `
あなたはデータ変換の専門家です。
「入力データ」の各列を、「完成形フォーマット」のヘッダー順序に合わせて列単位で並べ替え、
かつデータ形式を「完成形フォーマットのサンプル行」に合わせて整形してください。

この処理は大きなCSVを複数バッチに分割して実行しています。
列の意味や値の変換ルールは、すべてのバッチで必ず一貫して適用してください。

### 列推論のヒント
列の意味はデータ内容全体から判断してください。
特に以下の特徴は判断の手がかりになります。

・メールアドレス形式
・電話番号形式
・日付形式
・数値ID
・人名
・カテゴリ値

これらに当てはまらない列も、内容から適切に推測してください。

${headerMissing ? `
### 入力CSVについて
入力CSVにはヘッダーが存在しない可能性があります。
その場合は1行目もデータとして扱い、列の意味をデータ内容から推測してください。
` : ""}

### 1. 完成形フォーマット
- ヘッダー: ${JSON.stringify(baseHeader)}
- サンプル行（形式の参考。複数行）:
${baseSample.map(row => JSON.stringify(row)).join('\n')}

### 2. 変換用データ
- ヘッダー: ${JSON.stringify(convertHeader)}
- 入力データ（変換対象）:
${convertRows.map(row => JSON.stringify(row)).join('\n')}


- データの前後に ```csv、```、'''、"""、説明文、空行、注釈、見出しを一切含めないでください。
- データの先頭行は項目行またはデータ行、データの終了業はデータ行になります。

### 3. 変換の厳守ルール
- 「完成形フォーマット」の各ヘッダー項目に対し、入力データの中から意味的に最も適合する列を抽出し、指定の順序に並べ替えてください。
- 1行だけで判断せず、複数行で共通する形式を優先すること
- 日付、時間、電話番号、数値、ステータス表記などは、サンプル行の形式をテンプレートとし、項目単位で完全に統一してください。
- 言語が違う場合は翻訳して揺らぎを持たせて関連性を調べて下さい。表記方法は完成形フォーマットに一致させて下さい。
- ユーザー指示による数値比較は、文字を除去して数学的に評価すること。
- 入力データに対応する項目がない場合は空欄（""）としてください。
- 全角/半角の統一、不要な空白の削除を自動で行ってください。
- ユーザー指示: ${comment || "特になし"}

### 出力規定
- 出力は1行目に完成形フォーマットのヘッダーを1回だけ含むCSV本文のみとしてください。
- 2回目以降のヘッダー行は絶対に出力しないでください。
- 前置きや解説は一切含めず、純粋なCSVデータのみを返してください。
- データの前後に ```csv、```、'''、"""、説明文、空行、注釈、見出しを一切含めないでください。
- データの先頭行は項目行またはデータ行、データの終了業はデータ行になります。
`;

        const response = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${MODEL_NAME}:generateContent?key=${API_KEY}`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                contents: [{ parts: [{ text: prompt }] }],
                generationConfig: {
                    temperature: 0,
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