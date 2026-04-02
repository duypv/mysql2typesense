import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: ["test/**/*.test.ts"],
    testTimeout: 15000,
    globals: false,
    // E2E tests with polling can take up to 30s per test — allowed via per-test timeout override
    hookTimeout: 20000,
    coverage: {
      provider: "v8",
      include: ["src/**/*.ts"],
      exclude: ["src/app/**", "src/types/**"]
    }
  }
});
