import { createServer } from "node:http";

const port = Number(process.env.PORT || 3000);

const server = createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ status: "ok" }));
    return;
  }
  res.writeHead(404, { "content-type": "application/json" });
  res.end(JSON.stringify({ error: "not found" }));
});

// Stop promptly on SIGTERM so Relay's reconciler replaces the container without
// waiting for the daemon's SIGKILL grace period.
process.on("SIGTERM", () => server.close(() => process.exit(0)));

server.listen(port, "0.0.0.0", () => {
  console.log(`custom-build-service listening on ${port}`);
});
