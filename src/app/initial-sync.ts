import { bootstrap } from "./bootstrap.js";

async function main() {
  const app = await bootstrap();

  try {
    await app.initialSyncService.run(app.tables);
    app.logger.info("Initial sync completed");
  } finally {
    await app.dispose();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});