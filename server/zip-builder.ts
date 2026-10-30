/** Minimal stored-entry ZIP for tests: [name, bytes] entries, no compression. */
export function buildZip(entries: [string, Uint8Array][]): Uint8Array {
  const chunks: Uint8Array[] = [];
  const central: Uint8Array[] = [];
  const u32 = (v: number) => new Uint8Array([v & 255, (v >>> 8) & 255, (v >>> 16) & 255, (v >>> 24) & 255]);
  const u16 = (v: number) => new Uint8Array([v & 255, (v >>> 8) & 255]);
  const cat = (...parts: Uint8Array[]) => {
    const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
    let at = 0;
    for (const p of parts) { out.set(p, at); at += p.length; }
    return out;
  };
  let offset = 0;
  for (const [name, bytes] of entries) {
    const nameBytes = new TextEncoder().encode(name);
    const local = cat(
      u32(0x04034b50), u16(20), u16(0), u16(0), u16(0), u16(0),
      u32(bytes.length), u32(bytes.length),
      u16(nameBytes.length), u16(0), nameBytes, bytes,
    );
    central.push(cat(
      u32(0x02014b50), u16(20), u16(20), u16(0), u16(0), u16(0), u16(0),
      u32(bytes.length), u32(bytes.length),
      u16(nameBytes.length), u16(0), u16(0), u16(0), u16(0),
      u32(0), u32(offset), nameBytes,
    ));
    chunks.push(local);
    offset += local.length;
  }
  const centralAll = cat(...central);
  chunks.push(centralAll);
  chunks.push(cat(
    u32(0x06054b50), u16(0), u16(0), u16(central.length), u16(central.length),
    u32(centralAll.length), u32(offset), u16(0),
  ));
  return cat(...chunks);
}
