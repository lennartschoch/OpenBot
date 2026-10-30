const ws = new WebSocket("ws://localhost:3101/api/intelligence-gateway/runner", ["phoenix"]);
ws.onopen = () => console.log("open protocol:", ws.protocol);
ws.onclose = (e) => console.log("close", e.code);
ws.onerror = () => console.log("error");
setTimeout(() => process.exit(0), 2000);
