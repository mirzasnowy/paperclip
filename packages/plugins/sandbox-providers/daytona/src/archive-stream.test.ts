import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import type { DuplexChannelSession } from "./duplex-command-stream.js";

const { openSession } = vi.hoisted(() => ({ openSession: vi.fn() }));
vi.mock("./duplex-command-stream.js", () => ({ openDaytonaDuplexChannelSession: openSession }));
import { streamSandboxArchive } from "./archive-stream.js";

const roots: string[] = [];
afterEach(async () => {
  vi.resetAllMocks();
  await Promise.all(roots.splice(0).map(root => fs.rm(root, { recursive: true, force: true })));
});

it.each(["missing receipt", "tar failure", "transport loss", "invalid base64", "oversized frame", "timeout"])("rejects %s and closes the transfer", async fault => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "archive-stream-"));
  roots.push(root);
  const close = vi.fn(async () => {});
  openSession.mockImplementation(async (_process, command: string[]) => {
    const marker = command.join(" ").match(/paperclip-archive-[a-f0-9-]+/)![0];
    let listener: (bytes: Uint8Array) => void;
    let end!: (exit: { exitCode: number | null; transportClosed: boolean }) => void;
    const completion = new Promise<{ exitCode: number | null; transportClosed: boolean }>(resolve => { end = resolve; });
    let writes = 0;
    const emit = (text: string) => listener(Buffer.from(text));
    return {
      onData: (callback: typeof listener) => { listener = callback; emit(`${marker}:ready\n`); },
      write: () => {
        queueMicrotask(() => {
          if (++writes === 1) {
            emit(`${marker}:start\n`);
            if (fault === "timeout") return;
            if (fault === "invalid base64") emit("data:not!base64\n");
            else if (fault === "oversized frame") emit(`data:${"a".repeat(6 * 1024 * 1024)}`);
            else emit(`data:${Buffer.from("partial archive").toString("base64")}\n`);
          } else {
            if (fault === "tar failure") emit(`${marker}:done\n`);
            end({ exitCode: fault === "tar failure" ? 2 : fault === "transport loss" ? null : 0, transportClosed: fault === "transport loss" });
          }
        });
      },
      wait: () => completion,
      kill: () => {},
      close,
    } satisfies DuplexChannelSession;
  });
  await expect(streamSandboxArchive({ process: {} as never, cwd: "/workspace", archiveScript: "tar -cf - .", destination: path.join(root, "archive.tar"), timeoutSeconds: fault === "timeout" ? 0.02 : 5 })).rejects.toThrow(/archive stream/i);
  expect(close).toHaveBeenCalledOnce();
});

it("closes a session that connects after the deadline", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "archive-stream-"));
  roots.push(root);
  let connect!: (session: DuplexChannelSession) => void;
  openSession.mockReturnValue(new Promise<DuplexChannelSession>(resolve => { connect = resolve; }));
  await expect(streamSandboxArchive({ process: {} as never, cwd: "/workspace", archiveScript: "tar -cf - .", destination: path.join(root, "archive.tar"), timeoutSeconds: 0.02 })).rejects.toThrow("timed out");
  const close = vi.fn(async () => {});
  const onData = vi.fn();
  connect({ close, onData, write: vi.fn(), wait: vi.fn(), kill: vi.fn() });
  await vi.waitFor(() => expect(close).toHaveBeenCalledOnce());
  expect(onData).not.toHaveBeenCalled();
});
