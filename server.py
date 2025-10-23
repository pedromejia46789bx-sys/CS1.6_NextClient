# server.py — Render-ready
# Sirve estático + /download que: reconstruye ZIP desde volúmenes -> extrae -> recomprime -> cachea -> transmite.
# Librería estándar únicamente.

import io
import os
import sys
import shutil
import posixpath
import tempfile
from datetime import datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
from zipfile import ZipFile, ZIP_DEFLATED

# ========= CONFIG =========
HOST        = "0.0.0.0"
PORT        = int(os.getenv("PORT", "8080"))
BASE_DIR    = os.path.abspath(os.path.dirname(__file__))
PUBLIC_DIR  = BASE_DIR
PARTS_DIR   = os.path.join(PUBLIC_DIR, "files")

FILE_BASE   = "CS1.6_NextClient"   # nombre base SIN extensión
PART_COUNT  = 26                    # ajusta si cambia
EXT_PAD     = 2                     # z01..z09 => 2; si fuera z001 => 3
CHUNK_SIZE  = 2 * 1024 * 1024       # 2 MiB por chunk
CACHE_DIR   = os.path.join(PUBLIC_DIR, ".cache")  # efímero en Render, persiste por deploy
READY_ZIP   = os.path.join(CACHE_DIR, f"{FILE_BASE}_ready.zip")
# ==========================


def part_path(i: int) -> str:
    return os.path.join(PARTS_DIR, f"{FILE_BASE}.z{str(i).zfill(EXT_PAD)}")

def last_zip_path() -> str:
    return os.path.join(PARTS_DIR, f"{FILE_BASE}.zip")

def ensure_parts_exist():
    missing = []
    for i in range(1, PART_COUNT + 1):
        p = part_path(i)
        if not os.path.exists(p):
            missing.append(os.path.relpath(p, PUBLIC_DIR))
    lz = last_zip_path()
    if not os.path.exists(lz):
        missing.append(os.path.relpath(lz, PUBLIC_DIR))
    if missing:
        raise FileNotFoundError("Faltan archivos:\n- " + "\n- ".join(missing))

def latest_parts_mtime() -> float:
    """Devuelve el mtime más reciente entre todos los volúmenes."""
    paths = [part_path(i) for i in range(1, PART_COUNT + 1)] + [last_zip_path()]
    return max(os.path.getmtime(p) for p in paths)

def rebuild_ready_zip(force: bool = False) -> str:
    """
    Reconstruye el ZIP a partir de los volúmenes, lo extrae y re-comprime en un ZIP normal único.
    Usa caché si READY_ZIP ya existe y es más nuevo que las partes.
    Devuelve la ruta absoluta del *_ready.zip.
    """
    ensure_parts_exist()
    os.makedirs(CACHE_DIR, exist_ok=True)

    parts_mtime = latest_parts_mtime()
    ready_exists = os.path.exists(READY_ZIP)

    if ready_exists and not force:
        if os.path.getsize(READY_ZIP) > 0 and os.path.getmtime(READY_ZIP) >= parts_mtime:
            # Cache válida
            return READY_ZIP

    # Carpetas temporales de trabajo
    work_dir = tempfile.mkdtemp(prefix="rebuild_", dir=CACHE_DIR)
    combined_zip = os.path.join(work_dir, f"{FILE_BASE}_combined.zip")
    extract_dir  = os.path.join(work_dir, "extract")
    os.makedirs(extract_dir, exist_ok=True)

    try:
        # 1) Reconstruir ZIP combinando z01..zNN + .zip
        with open(combined_zip, "wb") as out:
            for i in range(1, PART_COUNT + 1):
                with open(part_path(i), "rb") as f:
                    shutil.copyfileobj(f, out, CHUNK_SIZE)
            with open(last_zip_path(), "rb") as f:
                shutil.copyfileobj(f, out, CHUNK_SIZE)

        # 2) Validar y extraer
        with ZipFile(combined_zip) as zc:
            # Si el archivo multi-volumen está bien, esto listará sin error:
            _ = zc.namelist()
            zc.extractall(extract_dir)

        # 3) Re-comprimir a ZIP normal único
        tmp_ready = READY_ZIP + ".tmp"
        with ZipFile(tmp_ready, "w", compression=ZIP_DEFLATED) as z:
            for root, _, files in os.walk(extract_dir):
                for name in files:
                    abs_path = os.path.join(root, name)
                    arcname  = os.path.relpath(abs_path, extract_dir).replace("\\", "/")
                    z.write(abs_path, arcname)

        # 4) Atomizar reemplazo del cache
        if os.path.exists(READY_ZIP):
            os.remove(READY_ZIP)
        os.replace(tmp_ready, READY_ZIP)

        # Ajustar mtime para saber que es nuevo
        os.utime(READY_ZIP, (parts_mtime, parts_mtime))
        return READY_ZIP

    finally:
        # Limpiar trabajo temporal
        shutil.rmtree(work_dir, ignore_errors=True)

def stream_file(handler: SimpleHTTPRequestHandler, path: str, download_name: str):
    """Envía 'path' con headers correctos y stream en chunks."""
    length = os.path.getsize(path)
    handler.send_response(200)
    handler.send_header("Content-Type", "application/zip")
    handler.send_header("Content-Disposition", f'attachment; filename="{download_name}"')
    handler.send_header("Content-Length", str(length))
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Connection", "keep-alive")
    handler.send_header("X-Accel-Buffering", "no")
    handler.end_headers()

    with open(path, "rb", buffering=0) as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            try:
                handler.wfile.write(chunk)
            except (BrokenPipeError, ConnectionResetError):
                # Cliente canceló: salimos en silencio
                return

class Handler(SimpleHTTPRequestHandler):
    # Sirve desde PUBLIC_DIR
    def translate_path(self, path):
        path = urlparse(path).path
        path = posixpath.normpath(path)
        words = [w for w in path.split('/') if w]
        out = PUBLIC_DIR
        for w in words:
            _, w = os.path.split(w)
            if w in (os.curdir, os.pardir):
                continue
            out = os.path.join(out, w)
        return out

    def do_GET(self):
        if self.path.startswith("/download"):
            return self._handle_download()
        if self.path.startswith("/rebuild"):
            return self._handle_rebuild()  # fuerza reconstrucción manual
        if self.path.startswith("/health"):
            return self._ok_text("ok")
        if self.path in ("/", ""):
            self.path = "/index.html"
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

    def _handle_download(self):
        try:
            ready = rebuild_ready_zip(force=False)
            name  = os.path.basename(READY_ZIP)
            return stream_file(self, ready, name)
        except FileNotFoundError as e:
            return self._ok_text(f"NO_ENCONTRADAS:\n{e}", code=404)
        except Exception as e:
            # Si algo falla, devolvemos error legible (sin romper conexión)
            return self._ok_text(f"ERROR: {e}", code=500)

    def _handle_rebuild(self):
        try:
            ready = rebuild_ready_zip(force=True)
            size_mb = os.path.getsize(ready)/1048576
            return self._ok_text(f"REBUILT: {os.path.basename(ready)} ({size_mb:.2f} MiB)")
        except FileNotFoundError as e:
            return self._ok_text(f"NO_ENCONTRADAS:\n{e}", code=404)
        except Exception as e:
            return self._ok_text(f"ERROR: {e}", code=500)

def main():
    os.chdir(PUBLIC_DIR)
    httpd = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"Serving at http://0.0.0.0:{PORT}  (root: {PUBLIC_DIR})")
    print("Endpoints: /  /download  /rebuild  /health")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nBye!")

if __name__ == "__main__":
    if len(sys.argv) >= 2:
        PORT = int(sys.argv[1])
    main()
