import { bootstrap } from "./bootstrap.js";

async function main() {
  const app = await bootstrap();

  process.on("SIGINT", async () => {
    await app.dispose();
    process.exit(0);
  });

  process.on("SIGTERM", async () => {
    await app.dispose();
    process.exit(0);
  });

  await app.realtimeSyncService.run();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});