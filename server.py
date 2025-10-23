# server.py — Render-ready con 7-Zip (p7zip-full)
# /download-raw : extrae el ZIP multivolumen usando el archivo-guía CS1.6_NextClient.zip y entrega el artefacto principal SIN comprimir
# /rebuild      : fuerza re-extracción (debug)
# /concat       : concatena volúmenes y entrega binario crudo (por si alguna vez lo necesitas)
# /diag         : diagnóstico de partes y de 7z
# /health       : ok

import os, sys, shutil, posixpath, tempfile, mimetypes, subprocess
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

# ========= CONFIG =========
HOST        = "0.0.0.0"
PORT        = int(os.getenv("PORT", "8080"))
BASE_DIR    = os.path.abspath(os.path.dirname(__file__))
PUBLIC_DIR  = BASE_DIR
PARTS_DIR   = os.path.join(PUBLIC_DIR, "files")

FILE_BASE   = "CS1.6_NextClient"   # sin extensión
PART_COUNT  = 26                    # .z01 .. .z26
EXT_PAD     = 2                     # z01..z09 => 2
CHUNK_SIZE  = 2 * 1024 * 1024

CACHE_DIR   = os.path.join(PUBLIC_DIR, ".cache")
EXTRACT_DIR = os.path.join(CACHE_DIR, "extract")
CONCAT_TMP  = os.path.join(CACHE_DIR, "__concat_tmp__")

# Si quieres forzar un nombre específico como artefacto (ej. "NextClient_Setup.exe"),
# ponlo aquí. Si None, se elegirá el más grande.
PREFERRED_ARTIFACT_NAME = None  # ejemplo: "NextClient_Setup.exe"

# ==========================

LFS_SIGNATURE = b"version https://git-lfs.github.com/spec"

def pp(p): return os.path.relpath(p, PUBLIC_DIR).replace("\\", "/")
def part_path(i): return os.path.join(PARTS_DIR, f"{FILE_BASE}.z{str(i).zfill(EXT_PAD)}")
def last_zip_path(): return os.path.join(PARTS_DIR, f"{FILE_BASE}.zip")

def file_head(path, n=96):
    try:
        with open(path, "rb") as f: return f.read(n)
    except Exception: return b""

def ensure_parts_exist():
    paths = [*map(part_path, range(1, PART_COUNT+1)), last_zip_path()]
    missing = [pp(p) for p in paths if not os.path.exists(p)]
    if missing:
        raise RuntimeError("Faltan archivos:\n- " + "\n- ".join(missing))

def ensure_not_lfs():
    paths = [*map(part_path, range(1, PART_COUNT+1)), last_zip_path()]
    suspects = [pp(p) for p in paths if file_head(p).startswith(LFS_SIGNATURE)]
    if suspects:
        raise RuntimeError(
            "Se detectaron PUNTEROS Git LFS (no binarios):\n- " + "\n- ".join(suspects) +
            "\nSolución: usa build command con 'git lfs pull' o sube binarios reales."
        )

def latest_parts_mtime():
    paths = [*map(part_path, range(1, PART_COUNT+1)), last_zip_path()]
    return max(os.path.getmtime(p) for p in paths)

def ensure_7z():
    try:
        out = subprocess.check_output(["7z", "-h"], stderr=subprocess.STDOUT, text=True, timeout=10)
        return out.splitlines()[0].strip()
    except Exception as e:
        raise RuntimeError("7z no está disponible. Instala con: apt-get update && apt-get install -y p7zip-full") from e

def extract_with_7z(force=False):
    """
    Usa 7-Zip para extraer ZIP multivolumen tomando como guía *exactamente* files/CS1.6_NextClient.zip.
    Reutiliza caché si la extracción está al día.
    """
    ensure_parts_exist()
    ensure_not_lfs()
    ensure_7z()
    os.makedirs(CACHE_DIR, exist_ok=True)

    parts_mtime = latest_parts_mtime()
    if os.path.isdir(EXTRACT_DIR) and not force:
        try:
            if os.path.getmtime(EXTRACT_DIR) >= parts_mtime and any(
                os.path.isfile(os.path.join(EXTRACT_DIR, f)) for f in os.listdir(EXTRACT_DIR)
            ):
                return EXTRACT_DIR
        except Exception:
            pass

    # limpiar y extraer
    if os.path.isdir(EXTRACT_DIR):
        shutil.rmtree(EXTRACT_DIR, ignore_errors=True)
    os.makedirs(EXTRACT_DIR, exist_ok=True)

    # Ejecutar 7z en PARTS_DIR y apuntar al último volumen (.zip)
    cmd = ["7z", "x", "-y", f"-o{EXTRACT_DIR}", os.path.basename(last_zip_path())]
    try:
        proc = subprocess.run(cmd, cwd=PARTS_DIR, capture_output=True, text=True, timeout=None)
    except Exception as e:
        raise RuntimeError(f"No se pudo ejecutar 7z: {e}")

    if proc.returncode != 0:
        raise RuntimeError(f"7z falló (code={proc.returncode}).\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")

    os.utime(EXTRACT_DIR, (parts_mtime, parts_mtime))
    return EXTRACT_DIR

def pick_main_artifact(folder):
    """
    Si PREFERRED_ARTIFACT_NAME está definido, lo busca por nombre (case-insensitive).
    Si no lo encuentra, toma el archivo más grande.
    """
    # 1) por nombre preferido
    if PREFERRED_ARTIFACT_NAME:
        target = PREFERRED_ARTIFACT_NAME.lower()
        for root, _, files in os.walk(folder):
            for name in files:
                if name.lower() == target:
                    return os.path.join(root, name)

    # 2) por tamaño (más grande)
    best, best_size = None, -1
    for root, _, files in os.walk(folder):
        for name in files:
            p = os.path.join(root, name)
            sz = os.path.getsize(p)
            if sz > best_size:
                best_size, best = sz, p
    if not best:
        raise RuntimeError("La extracción no produjo archivos.")
    return best

def concat_volumes(out_path):
    """Concatena volúmenes (por si alguna vez necesitas entregar el binario crudo)."""
    ensure_parts_exist()
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "wb") as out:
        for i in range(1, PART_COUNT + 1):
            with open(part_path(i), "rb") as f:
                shutil.copyfileobj(f, out, CHUNK_SIZE)
        with open(last_zip_path(), "rb") as f:
            shutil.copyfileobj(f, out, CHUNK_SIZE)

def stream_file(handler, path, name):
    ctype, _ = mimetypes.guess_type(name)
    if not ctype: ctype = "application/octet-stream"
    length = os.path.getsize(path)
    handler.send_response(200)
    handler.send_header("Content-Type", ctype)
    handler.send_header("Content-Disposition", f'attachment; filename="{name}"')
    handler.send_header("Content-Length", str(length))
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Connection", "keep-alive")
    handler.send_header("X-Accel-Buffering", "no")
    handler.end_headers()
    with open(path, "rb", buffering=0) as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk: break
            try:
                handler.wfile.write(chunk)
            except (BrokenPipeError, ConnectionResetError):
                return

def diag_report():
    lines = []
    lines.append(f"ROOT: {PUBLIC_DIR}")
    lines.append(f"PARTS_DIR: {pp(PARTS_DIR)}")
    lines.append(f"CACHE_DIR: {pp(CACHE_DIR)}")
    lines.append(f"BASE: {FILE_BASE}, PARTS: {PART_COUNT}, PAD: {EXT_PAD}")
    # 7z
    try:
        lines.append("7z: " + ensure_7z())
    except Exception as e:
        lines.append("7z: NO DISPONIBLE -> " + str(e))
    # partes
    ok = True
    for i in range(1, PART_COUNT+1):
        p = part_path(i)
        if not os.path.exists(p):
            lines.append(f"[MISS] {pp(p)}"); ok = False
        else:
            lines.append(f"[OK]   {pp(p)}  size={os.path.getsize(p)}")
    p = last_zip_path()
    if not os.path.exists(p):
        lines.append(f"[MISS] {pp(p)}"); ok = False
    else:
        lines.append(f"[OK]   {pp(p)}  size={os.path.getsize(p)}")
    lines.append("Status: " + ("OK" if ok else "FALTAN PARTES"))
    # extract preview
    if os.path.isdir(EXTRACT_DIR):
        lines.append("\nExtract dir:")
        shown = 0
        for root, _, files in os.walk(EXTRACT_DIR):
            for name in files:
                path = os.path.join(root, name)
                lines.append(f"  - {pp(path)} ({os.path.getsize(path)} bytes)")
                shown += 1
                if shown >= 8: break
            break
    return "\n".join(lines)

class Handler(SimpleHTTPRequestHandler):
    def translate_path(self, path):
        path = urlparse(path).path
        path = posixpath.normpath(path)
        words = [w for w in path.split('/') if w]
        out = PUBLIC_DIR
        for w in words:
            _, w = os.path.split(w)
            if w in (os.curdir, os.pardir): continue
            out = os.path.join(out, w)
        return out

    def do_GET(self):
        if self.path.startswith("/download-raw"): return self._handle_download_raw()
        if self.path.startswith("/rebuild"):      return self._handle_rebuild()
        if self.path.startswith("/concat"):       return self._handle_concat()
        if self.path.startswith("/diag"):         return self._ok_text(diag_report())
        if self.path.startswith("/health"):       return self._ok_text("ok")
        if self.path in ("/", ""): self.path = "/index.html"
        return super().do_GET()

    def _ok_text(self, text, code=200):
        data = text.encode("utf-8")
        try:
            self.send_response(code)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _handle_download_raw(self):
        """Extrae con 7z usando el archivo-guía CS1.6_NextClient.zip y entrega el artefacto principal SIN comprimir."""
        try:
            folder = extract_with_7z(force=False)
            mainf  = pick_main_artifact(folder)
            return stream_file(self, mainf, os.path.basename(mainf))
        except Exception as e:
            return self._ok_text(f"ERROR: {e}", code=500)

    def _handle_rebuild(self):
        """Fuerza re-extracción (debug)."""
        try:
            folder = extract_with_7z(force=True)
            mainf  = pick_main_artifact(folder)
            return self._ok_text(f"REBUILT OK\nMain: {pp(mainf)} ({os.path.getsize(mainf)} bytes)")
        except Exception as e:
            return self._ok_text(f"ERROR: {e}", code=500)

    def _handle_concat(self):
        """Concatena y entrega el binario crudo (por si alguna vez prefieres esta opción)."""
        try:
            qs   = parse_qs(urlparse(self.path).query or "")
            name = qs.get("name", [f"{FILE_BASE}.bin"])[0]
            os.makedirs(CACHE_DIR, exist_ok=True)
            concat_volumes(CONCAT_TMP)
            try:
                return stream_file(self, CONCAT_TMP, name)
            finally:
                try: os.remove(CONCAT_TMP)
                except Exception: pass
        except Exception as e:
            return self._ok_text(f"ERROR: {e}", code=500)

def main():
    os.chdir(PUBLIC_DIR)
    httpd = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"Serving at http://0.0.0.0:{PORT}  (root: {PUBLIC_DIR})")
    print("Endpoints:")
    print("  /             -> index.html")
    print("  /download-raw -> extrae con 7z desde files/CS1.6_NextClient.zip (guía) y entrega artefacto principal (sin comprimir)")
    print("  /rebuild      -> fuerza re-extracción")
    print("  /concat?name=CS1.6_NextClient.exe -> concatena y entrega binario crudo (opcional)")
    print("  /diag         -> diagnóstico")
    print("  /health       -> ok")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nBye!")

if __name__ == "__main__":
    main()
