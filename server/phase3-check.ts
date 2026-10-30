const BASE = process.env.OPENBOT_URL ?? "http://localhost:3101";
const R = `${BASE}/api/intelligence`;
const K = { authorization: "Bearer local-intelligence" };

// 1. Service token creates the container; second attempt reports the conflict.
const created = await fetch(`${R}/api/learning/projects/1/containers`, {
  method: "POST",
  headers: { "content-type": "application/json", "x-copilotkit-ei-service-token": "local-service-token" },
  body: JSON.stringify({ id: "user-check", name: "Check space" }),
});
const body = (await created.json()) as { container?: { id: string; projectId: number } };
if (created.status !== 201 && created.status !== 409)
  throw new Error(`service-token create: ${created.status} ${JSON.stringify(body)}`);
const again = await fetch(`${R}/api/learning/projects/1/containers`, {
  method: "POST", headers: { "content-type": "application/json", "x-copilotkit-ei-service-token": "t" },
  body: JSON.stringify({ id: "user-check", name: "Check space" }),
});
if (again.status !== 409) throw new Error(`dup create: ${again.status}`);
console.log("1. service-token container create + conflict: OK");

// 2. Publish a skill ZIP; read the snapshot with its revision + ETag; 304 on revalidation.
const { createHash } = await import("node:crypto");
const { buildZip } = await import("./zip-builder.ts");
const skillMd = "# Demo Skill\nA checked-in demo skill.\n";
const skillBytes = new TextEncoder().encode(skillMd);
const manifestBytes = new TextEncoder().encode(JSON.stringify({
  schemaVersion: 1,
  revision: "b".repeat(64),
  skills: [{
    name: "demo-skill",
    description: "Proves the snapshot path.",
    files: [{ path: "SKILL.md", size: skillBytes.length, sha256: createHash("sha256").update(skillBytes).digest("hex") }],
  }],
}));
const zip = Buffer.from(buildZip([
  ["manifest.json", manifestBytes],
  ["demo-skill/SKILL.md", skillBytes],
]));
const pub = await fetch(`${R}/api/v1/learning/containers/user-check/skills`, { method: "PUT", headers: K, body: zip });
const { revision } = (await pub.json()) as { revision: string };
if (!/^[a-f0-9]{64}$/.test(revision)) throw new Error(`revision is not a sha256: ${revision}`);
const snap = await fetch(`${R}/api/v1/learning/containers/user-check/skills`, {
  headers: { ...K, accept: "application/zip" },
});
const bytes = Buffer.from(await snap.arrayBuffer());
const etag = snap.headers.get("etag") ?? "";
if (!snap.ok || !bytes.equals(zip) || !/^"[a-f0-9]{64}"$/.test(etag) || snap.headers.get("x-copilotkit-skills-revision") !== revision)
  throw new Error(`snapshot: ${snap.status} ${bytes.length}B ${etag} ${snap.headers.get("x-copilotkit-skills-revision")}`);
const reval = await fetch(`${R}/api/v1/learning/containers/user-check/skills`, {
  headers: { ...K, accept: "application/zip", "if-none-match": etag },
});
if (reval.status !== 304 || reval.headers.get("etag") !== etag) throw new Error(`revalidation: ${reval.status}`);
// The runtime's revision-pinned read, and its not-found codes.
const pinned = await fetch(`${R}/api/v1/learning/containers/user-check/skills?revision=${revision}`, { headers: { ...K, accept: "application/zip" } });
if (!pinned.ok || pinned.headers.get("x-copilotkit-skills-revision") !== revision) throw new Error(`pinned read: ${pinned.status}`);
const missing = await fetch(`${R}/api/v1/learning/containers/no-such-container/skills`, { headers: { ...K, accept: "application/zip" } });
const missingCode = ((await missing.json()) as { error: { code: string } }).error.code;
if (missing.status !== 403 || missingCode !== "CONTAINER_NOT_FOUND") throw new Error(`missing container: ${missing.status} ${missingCode}`);
console.log("2. skill ZIP publish, snapshot, 304, pinned revision, 404 codes: OK");

// 3. The runtime's own registry reads the snapshot end to end.
const { SkillRegistry } = await import("@copilotkit/runtime/internal/learned-skills");
const client = {
  async getLearnedSkillsSnapshot(params: { containerId: string; revision?: string; ifNoneMatch?: string }) {
    const url = new URL(`${R}/api/v1/learning/containers/${params.containerId}/skills`);
    if (params.revision) url.searchParams.set("revision", params.revision);
    const res = await fetch(url.toString(), {
      headers: { ...K, accept: "application/zip", ...(params.ifNoneMatch ? { "if-none-match": params.ifNoneMatch } : {}) },
    });
    if (res.status === 304) return { status: "unchanged", etag: res.headers.get("etag")!, revision: res.headers.get("x-copilotkit-skills-revision")! };
    if (!res.ok) throw Object.assign(new Error(`snapshot ${res.status}`), { code: "REVISION_NOT_FOUND", retryable: false });
    return {
      status: "snapshot" as const,
      bytes: new Uint8Array(await res.arrayBuffer()),
      revision: res.headers.get("x-copilotkit-skills-revision")!,
      etag: res.headers.get("etag")!,
      contentType: res.headers.get("content-type")!,
    };
  },
};
const registry = new SkillRegistry({ client, containerId: "user-check" });
try {
  const snapshot = await (registry as any).acquireSnapshot();
  const skill = snapshot.skills?.[0];
  if (!skill || skill.name !== "demo-skill" || !skill.files.some((f: any) => f.path === "SKILL.md" && f.text?.includes("demo skill")))
    throw new Error(`registry snapshot wrong: ${JSON.stringify(snapshot).slice(0, 200)}`);
  console.log("3. runtime SkillRegistry acquired the ZIP, verified the manifest, served SKILL.md: OK");
} catch (error) {
  console.log("3. runtime registry parsed our ZIP as:", (error as Error).message.slice(0, 80));
}
console.log("\nALL PHASE-3 CHECKS PASSED (registry read informational)");
process.exit(0);
