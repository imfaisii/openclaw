import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import type { PersistedUserTurnMessage } from "../../sessions/user-turn-transcript.types.js";
import {
  closeOpenClawAgentDatabasesForTest,
  openOpenClawAgentDatabase,
} from "../../state/openclaw-agent-db.js";
import { upsertSessionEntryCore } from "./session-accessor.js";
import {
  stageSessionPendingInput,
  type SessionPendingInputReceipt,
} from "./session-accessor.pending-inputs.js";
import { captureSessionPendingInputWorkerCustody } from "./session-accessor.sqlite-pending-inputs.js";
import { resolveSqliteScope, toDatabaseOptions } from "./session-accessor.sqlite-scope.js";
import { useTempSessionsFixture } from "./test-helpers.js";

describe("accepted input worker custody", () => {
  const fixture = useTempSessionsFixture("openclaw-pending-worker-custody-");
  let receipt: SessionPendingInputReceipt | undefined;

  afterEach(() => {
    receipt?.finish("interrupted");
    receipt = undefined;
    closeOpenClawAgentDatabasesForTest();
  });

  it("captures the canonical database path across a state-directory alias", async () => {
    const fixtureRoot = path.resolve(fixture.sessionsDir(), "../../..");
    const aliasRoot = path.join(fixtureRoot, "state-alias");
    fs.symlinkSync(fixtureRoot, aliasRoot, process.platform === "win32" ? "junction" : "dir");
    const scope = {
      agentId: "alias-agent",
      env: { OPENCLAW_STATE_DIR: aliasRoot },
      sessionId: "alias-session",
      sessionKey: "agent:alias-agent:pending-inputs",
    };
    await upsertSessionEntryCore(scope, { sessionId: scope.sessionId, updatedAt: 1 });
    const message: PersistedUserTurnMessage = {
      role: "user",
      content: "Continue through worker custody",
      timestamp: 100,
      idempotencyKey: "worker-alias:user",
    };
    receipt = await stageSessionPendingInput(scope, {
      runId: "worker-alias",
      message,
      assertCurrent: () => {},
    });
    if (!receipt) {
      throw new Error("Expected aliased pending input custody");
    }

    const custody = receipt.run(() => captureSessionPendingInputWorkerCustody());
    const database = openOpenClawAgentDatabase(toDatabaseOptions(resolveSqliteScope(scope)));
    expect(custody?.facts.databasePath).toBe(fs.realpathSync(database.path));
  });
});
