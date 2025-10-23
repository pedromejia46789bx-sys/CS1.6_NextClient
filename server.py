# server.py — Render-ready
# Reconstruye ZIP multivolumen -> EXTRAe -> sirve el artefacto principal SIN comprimir.
# Incluye caché, diagnóstico y endpoints auxiliares. Librería estándar.

import io
import os
import sys
import shutil
import posixpath
import tempfile
import mimetypes
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
from zipfile import ZipFile

# ========= CONFIG =========
HOST        = "0.0.0.0"
PORT        = int(os.getenv("PORT", "8080"))
BASE_DIR    = os.path.abspath(os.path.dirname(__file__))
PUBLIC_DIR  = BASE_DIR
PARTS_DIR   = os.path.join(PUBLIC_DIR, "files")

FILE_BASE   = "CS1.6_NextClient"   # nombre base SIN extensión
PART_COUNT  = 26                    # cuántas .zXX hay
EXT_PAD     = 2                     # z01..z09 => 2; si fuera z001 => 3
CHUNK_SIZE  = 2 * 1024 * 1024       # 2 MiB por chunk

CACHE_DIR   = os.path.join(PUBLIC_DIR, ".cache")
COMBINED_ZIP= os.path.join(CACHE_DIR, f"{FILE_BASE}_combined.zip")
EXTRACT_DIR = os.path.join(CACHE_DIR, "extract")  # aquí queda la extracción
# ==========================

LFS_SIGNATURE = b"version https://git-lfs.github.com/spec"

def pp(p):  # ruta relativa para mensajes
    return os.path.relpath(p, PUBLIC_DIR).replace("\\", "/")

def part_path(i: int) -> str:
    return os.path.join(PARTS_DIR, f"{FILE_BASE}.z{str(i).zfill(EXT_PAD)}")

def last_zip_path() -> str:
    return os.path.join(PARTS_DIR, f"{FILE_BASE}.zip")

def required_paths():
    return [part_path(i) for i in range(1, PART_COUNT + 1)] + [last_zip_path()]

def file_head(path: str, n: int = 80) -> bytes:
    try:
        with open(path, "rb") as f:
            return f.read(n)
    except Exception:
        return b""

def ensure_parts_exist_and_not_lfs():
    missing = []
    lfs = []
    tiny = []
    for p in required_paths():
        if not os.path.exists(p):
            missing.append(pp(p))
            continue
        sz = os.path.getsize(p)
        h  = file_head(p)
        if h.startswith(LFS_SIGNATURE):
            lfs.append(pp(p))
        if sz < 200:
            tiny.append(f"{pp(p)} ({sz} bytes)")
    if missing:
        raise RuntimeError("Faltan archivos:\n- " + "\n- ".join(missing))
    if lfs:
        raise RuntimeError(
            "Se detectaron PUNTEROS Git LFS (no binarios):\n- " +
            "\n- ".join(lfs) +
            "\nSolución: en Render usa build command con 'git lfs pull' o sube binarios reales."
        )
    if tiny:
        # No bloqueamos, pero avisamos
        sys.stderr.write("Advertencia: archivos muy pequeños:\n" + "\n".join(tiny) + "\n")

def latest_parts_mtime() -> float:
    return max(os.path.getmtime(p) for p in required_paths())

def rebuild_and_extract(force: bool=False) -> str:
    """
    Concatena volúmenes -> valida -> EXTRAE a EXTRACT_DIR.
    Usa caché si la extracción es más nueva que las partes (y no se fuerza).
    Devuelve la ruta de la carpeta EXTRACT_DIR.
    """
    ensure_parts_exist_and_not_lfs()
    os.makedirs(CACHE_DIR, exist_ok=True)

    parts_mtime = latest_parts_mtime()
    if os.path.exists(EXTRACT_DIR) and not force:
        # si la extracción ya existe y es reciente, reutilizamos
        if os.path.getmtime(EXTRACT_DIR) >= parts_mtime and any(
            os.path.isfile(os.path.join(EXTRACT_DIR, f)) for f in os.listdir(EXTRACT_DIR)
        ):
            return EXTRACT_DIR

    # Limpiar extracción previa
    if os.path.isdir(EXTRACT_DIR):
        shutil.rmtree(EXTRACT_DIR, ignore_errors=True)
    os.makedirs(EXTRACT_DIR, exist_ok=True)

    # Construir combined zip temporal
    work_dir = tempfile.mkdtemp(prefix="rebuild_", dir=CACHE_DIR)
    combined = os.path.join(work_dir, f"{FILE_BASE}_combined.zip")
    try:
        with open(combined, "wb") as out:
            # .z01..zNN
            for i in range(1, PART_COUNT + 1):
                with open(part_path(i), "rb") as f:
                    shutil.copyfileobj(f, out, CHUNK_SIZE)
            # .zip final
            with open(last_zip_path(), "rb") as f:
                shutil.copyfileobj(f, out, CHUNK_SIZE)

        # Validar / extraer
        try:
            with ZipFile(combined) as zc:
                _ = zc.namelist()     # fuerza lectura
                zc.extractall(EXTRACT_DIR)
        except Exception as e:
            raise RuntimeError(
                f"No se pudo abrir el ZIP reconstruido: {e}\n"
                "Causas: no es ZIP dividido, orden/cantidad incorrecta o partes corruptas."
            )

        # Tocar mtime para cache
        os.utime(EXTRACT_DIR, (parts_mtime, parts_mtime))
        return EXTRACT_DIR
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)

def pick_main_artifact(folder: str) -> str:
    """
    Heurística: devolver el archivo más grande dentro de la extracción.
    Suelen ser .exe / .msi / .zip / .7z / .pkg etc.
    """
    best_path, best_size = None, -1
    for root, _, files in os.walk(folder):
        for name in files:
            p = os.path.join(root, name)
            sz = os.path.getsize(p)
            if sz > best_size:
                best_size = sz
                best_path = p
    if not best_path:
        raise RuntimeError("La extracción no produjo archivos.")
    return best_path

def stream_file(handler: SimpleHTTPRequestHandler, path: str, download_name: str):
    ctype, _ = mimetypes.guess_type(download_name)
    if not ctype:
        ctype = "application/octet-stream"
    length = os.path.getsize(path)
    handler.send_response(200)
    handler.send_header("Content-Type", ctype)
    handler.send_header("Content-Disposition", f'attachment; filename="{download_name}"')
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

def diag_report() -> str:
    lines = []
    lines.append(f"ROOT: {PUBLIC_DIR}")
    lines.append(f"PARTS_DIR: {pp(PARTS_DIR)}")
    lines.append(f"CACHE_DIR: {pp(CACHE_DIR)}")
    lines.append(f"BASE: {FILE_BASE}  PART_COUNT: {PART_COUNT}  PAD: {EXT_PAD}")
    lines.append("")
    ok = True
    for p in required_paths():
        rel = pp(p)
        if not os.path.exists(p):
            lines.append(f"[MISS] {rel}")
            ok = False
            continue
        sz = os.path.getsize(p)
        head = file_head(p, 64)
        head_hex = head[:8].hex(" ")
        tag = "OK"
        if head.startswith(LFS_SIGNATURE):
            tag = "GIT_LFS_POINTER!"
            ok = False
        lines.append(f"[{tag}] {rel}  size={sz}  head={head_hex}")
    lines.append("")
    if ok:
        lines.append("Archivos presentes. Si /download-raw falla, puede que no sean ZIP divididos válidos.")
    else:
        lines.append("Hay problemas (ver arriba).")
    # Listado simple de extracción si existe
    if os.path.isdir(EXTRACT_DIR):
        lines.append("\nExtract dir exists:")
        for root, _, files in os.walk(EXTRACT_DIR):
            for name in files[:8]:
                p = os.path.join(root, name)
                lines.append(f"  - {pp(p)} ({os.path.getsize(p)} bytes)")
            break
    return "\n".join(lines)

class Handler(SimpleHTTPRequestHandler):
    # Servir desde PUBLIC_DIR
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

    def _handle_rebuild(self):
        try:
            folder = rebuild_and_extract(force=True)
            # elegir artefacto principal
            art = pick_main_artifact(folder)
            return self._ok_text(f"REBUILT & EXTRACTED\nMain: {pp(art)} ({os.path.getsize(art)} bytes)")
        except Exception as e:
            return self._ok_text(f"ERROR: {e}", code=500)

    def _handle_download_raw(self):
        """
        Reconstruye+extrae si hace falta y envía el archivo principal SIN comprimir.
        Heurística: archivo más grande dentro de la extracción.
        """
        try:
            folder = rebuild_and_extract(force=False)
            art = pick_main_artifact(folder)
            name = os.path.basename(art)
            return stream_file(self, art, name)
        except Exception as e:
            return self._ok_text(f"ERROR: {e}", code=500)

def main():
    os.chdir(PUBLIC_DIR)
    httpd = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"Serving at http://0.0.0.0:{PORT}  (root: {PUBLIC_DIR})")
    print("Endpoints:")
    print("  /             -> index.html")
    print("  /download-raw -> reconstruye + extrae + entrega artefacto principal (sin comprimir)")
    print("  /rebuild      -> fuerza reconstrucción y extracción (debug)")
    print("  /diag         -> diagnóstico de volúmenes")
    print("  /health       -> ok")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nBye!")

if __name__ == "__main__":
    if len(sys.argv) >= 2:
        PORT = int(sys.argv[1])
    main()
