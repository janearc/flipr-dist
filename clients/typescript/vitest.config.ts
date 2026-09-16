import { defineConfig } from "vitest/config";

// The client is the thing every service trusts before it spends money, so the
// floor is high. It is still a floor and a tripwire, not a goal: the case list
// mirrors the Go client's, and that correspondence is what is actually being
// maintained here.
export default defineConfig({
  test: {
    coverage: {
      provider: "v8",
      include: ["flipr.ts"],
      thresholds: { lines: 90, functions: 90, branches: 85, statements: 90 },
    },
  },
});
