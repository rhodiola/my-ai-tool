export async function onRequest(context) {
    return new Response(JSON.stringify({ summary: "通信成功！" }), {
        headers: { "Content-Type": "application/json" }
    });
}