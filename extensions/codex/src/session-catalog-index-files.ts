import { setImmediate as nextTurn } from "node:timers/promises";
import type { CodexThread } from "./app-server/protocol.js";
import type {
  CodexCatalogIndexRow,
  CodexCatalogRolloutFingerprint,
} from "./session-catalog-index-row.js";
import {
  projectCodexCatalogThread,
  mergeCodexCatalogRolloutRow,
} from "./session-catalog-projection.js";
import {
  type CodexCatalogRolloutScanner,
  isCodexCatalogRolloutPathCovered,
} from "./session-catalog-rollout-scanner.js";
import {
  codexCatalogRolloutLogicalPath,
  readCodexCatalogRollout,
} from "./session-catalog-rollouts.js";

export async function reconcileCodexCatalogFiles(params: {
  root: string;
  scanner: CodexCatalogRolloutScanner;
  rows: ReadonlyMap<string, CodexCatalogIndexRow>;
  observedFiles: ReadonlyMap<string, CodexCatalogRolloutFingerprint>;
  isCurrent: (id: string) => boolean;
  assertCurrent: () => void;
  requestNativeRefresh: () => void;
  report: (error: unknown) => void;
  put: (row: CodexCatalogIndexRow) => void;
  remove: (id: string) => void;
}): Promise<Map<string, CodexCatalogRolloutFingerprint>> {
  const { root } = params;
  params.assertCurrent();
  const byPath = new Map<string, CodexCatalogIndexRow>();
  let processed = 0;
  for (const row of params.rows.values()) {
    if (row.rolloutPath) {
      byPath.set(codexCatalogRolloutLogicalPath(row.rolloutPath), row);
    }
    if (++processed % 128 === 0) {
      await nextTurn();
      params.assertCurrent();
    }
  }
  const { files, present } = await params.scanner.scan(new Set(byPath.keys()));
  params.assertCurrent();
  const observed = new Map(files);
  for (const [file, fingerprint] of files) {
    if (++processed % 128 === 0) {
      await nextTurn();
      params.assertCurrent();
    }
    const previous = byPath.get(codexCatalogRolloutLogicalPath(file));
    const known = params.observedFiles.get(file) ?? previous?.fingerprint;
    if (known?.mtimeMs === fingerprint.mtimeMs && known.size === fingerprint.size) {
      continue;
    }
    params.requestNativeRefresh();
    // Publish a new fingerprint only after its projection survives concurrent native updates.
    observed.delete(file);
    if (known) {
      observed.set(file, known);
    }
    let thread: CodexThread | undefined;
    try {
      thread = await readCodexCatalogRollout(root, file);
    } catch (error) {
      params.assertCurrent();
      params.report(error);
      continue;
    }
    params.assertCurrent();
    if (!thread) {
      observed.set(file, fingerprint);
      continue;
    }
    if (!params.isCurrent(thread.id)) {
      continue;
    }
    const existing = params.rows.get(thread.id);
    if (
      existing?.rolloutPath &&
      codexCatalogRolloutLogicalPath(existing.rolloutPath) !== codexCatalogRolloutLogicalPath(file)
    ) {
      // Reverts retain older immutable files with the same thread id. Only
      // native metadata may change which rollout the catalog considers current.
      observed.set(file, fingerprint);
      continue;
    }
    if (!existing && !thread.preview) {
      observed.set(file, fingerprint);
      continue;
    }
    thread.preview ||= existing?.preview;
    const projected = await projectCodexCatalogThread(thread, root);
    params.assertCurrent();
    if (!params.isCurrent(thread.id)) {
      continue;
    }
    observed.set(file, fingerprint);
    const row = projected.rows[0];
    if (!row) {
      continue;
    }
    params.put(mergeCodexCatalogRolloutRow(row, existing, fingerprint));
    await nextTurn();
  }
  for (const row of byPath.values()) {
    if (++processed % 128 === 0) {
      await nextTurn();
      params.assertCurrent();
    }
    if (
      row.rolloutPath &&
      isCodexCatalogRolloutPathCovered(root, row.rolloutPath) &&
      !present.has(codexCatalogRolloutLogicalPath(row.rolloutPath)) &&
      params.rows.get(row.threadId) === row
    ) {
      params.requestNativeRefresh();
      params.remove(row.threadId);
    }
  }
  return observed;
}
