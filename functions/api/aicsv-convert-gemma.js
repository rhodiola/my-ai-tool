export async function onRequestPost(context) {

    const request = context.request
    const env = context.env

    const body = await request.json()

    const {
        baseHeader,
        baseSample,
        convertHeader,
        convertSample,
        convertRows,
        comment
    } = body

    const prompt = `
You analyze CSV column mapping.

Base header:
${JSON.stringify(baseHeader)}

Base sample:
${JSON.stringify(baseSample)}

Source header:
${JSON.stringify(convertHeader)}

Source sample:
${JSON.stringify(convertSample)}

User rule:
${comment}

Return JSON only:
{
 "mapping": {
   "base_column": "source_column"
 }
}
`

    const res = await fetch(
        "https://api-inference-url/gemma-3-12b-it",
        {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Authorization": `Bearer ${env.GEMMA_API_KEY}`
            },
            body: JSON.stringify({
                inputs: prompt,
                parameters: {
                    temperature: 0.2,
                    max_new_tokens: 512
                }
            })
        }
    )

    const data = await res.json()

    return Response.json(data)
}