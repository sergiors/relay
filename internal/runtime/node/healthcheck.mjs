const http = require("http");
const port = Number(process.argv[1]);
const path = process.argv[2];
const timeout = Number(process.argv[3]);
const req = http.get({ host: "127.0.0.1", port, path, timeout }, (res) => {
  res.resume();
  res.on("end", () =>
    process.exit(res.statusCode >= 200 && res.statusCode < 400 ? 0 : 1),
  );
});
req.on("timeout", () => {
  req.destroy();
  process.exit(1);
});
req.on("error", () => process.exit(1));
