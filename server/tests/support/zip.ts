import { crc32, deflateRawSync } from "node:zlib";

/**
 * A ZIP for tests, in the two shapes a real publisher emits: stored entries
 * (`method: 0`, the default) and deflated ones (`deflate: true`, which is what
 * an archive built by the CLI or the runtime looks like).
 *
 * The platform reads a Skills archive's `manifest.json` directly out of the
 * ZIP's local file headers, so a fixture has to carry the real compression
 * method and both sizes — a name and some bytes are not enough to exercise the
 * inflate path that a deflated archive takes.
 *
 * Layouts are the ones in PKWARE APPNOTE 4.3.6: 30 fixed bytes then the name in
 * a local header, 46 then the name in a central entry, 22 in the end record.
 */
export function buildZip(
  entries: [string, Uint8Array][],
  options: { deflate?: boolean } = {},
): Uint8Array<ArrayBuffer> {
  const chunks: Uint8Array[] = [];
  const central: Uint8Array[] = [];
  const u32 = (v: number) =>
    new Uint8Array([
      v & 255,
      (v >>> 8) & 255,
      (v >>> 16) & 255,
      (v >>> 24) & 255,
    ]);
  const u16 = (v: number) => new Uint8Array([v & 255, (v >>> 8) & 255]);
  const cat = (...parts: Uint8Array[]) => {
    const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
    let at = 0;
    for (const part of parts) {
      out.set(part, at);
      at += part.length;
    }
    return out;
  };
  let offset = 0;
  for (const [name, bytes] of entries) {
    const nameBytes = new TextEncoder().encode(name);
    const method = options.deflate ? 8 : 0;
    const body = options.deflate
      ? new Uint8Array(deflateRawSync(bytes))
      : bytes;
    const checksum = crc32(bytes);
    const local = cat(
      u32(0x04034b50),
      u16(20),
      u16(0),
      u16(method),
      u16(0),
      u16(0),
      u32(checksum),
      u32(body.length),
      u32(bytes.length),
      u16(nameBytes.length),
      u16(0),
      nameBytes,
      body,
    );
    central.push(
      cat(
        u32(0x02014b50),
        u16(20),
        u16(20),
        u16(0),
        u16(method),
        u16(0),
        u16(0),
        u32(checksum),
        u32(body.length),
        u32(bytes.length),
        u16(nameBytes.length),
        u16(0),
        u16(0),
        u16(0),
        u16(0),
        u32(0),
        u32(offset),
        nameBytes,
      ),
    );
    chunks.push(local);
    offset += local.length;
  }
  const centralAll = cat(...central);
  chunks.push(centralAll);
  chunks.push(
    cat(
      u32(0x06054b50),
      u16(0),
      u16(0),
      u16(central.length),
      u16(central.length),
      u32(centralAll.length),
      u32(offset),
      u16(0),
    ),
  );
  return cat(...chunks);
}
