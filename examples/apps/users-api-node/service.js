import Fastify from "fastify";

const port = Number(process.env.PORT || 80);

const app = Fastify();

app.get("/health", async () => {
  return { status: "ok" };
});

app.get("/users", async () => {
  return [
    { id: "user_1", name: "Ada Lovelace" },
    { id: "user_2", name: "Grace Hopper" },
  ];
});

await app.listen({ port, host: "0.0.0.0" });
console.log(`users-api listening on ${port}`);