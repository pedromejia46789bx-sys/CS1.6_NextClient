# server.py — Servidor estático + ensamblador/descargador + extractor (Render-ready)
# Librería estándar únicamente (no depende de frameworks)

import io
import os
import sys
import posixpath
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
from zipfile import ZipFile

# ========= CONFIG =========
HOST        = "0.0.0.0"
PORT        = int(os.getenv("PORT", "8080"))  # Render define PORT
BASE_DIR    = os.path.abspath(os.path.dirname(__file__))
PUBLIC_DIR  = BASE_DIR
PARTS_DIR   = os.path.join(PUBLIC_DIR, "files")

FILE_BASE   = "CS1.6_NextClient"  # nombre base SIN extensión
PART_COUNT  = 26                   # ajusta si tus partes son más/menos
EXT_PAD     = 2                    # z01..z09 => 2; z001..z999 => 3
CHUNK_SIZE  = 2 * 1024 * 1024      # 2 MiB
# ==========================

def part_path(i: int) -> str:
    return os.path.join(PARTS_DIR, f"{FILE_BASE}.z{str(i).zfill(EXT_PAD)}")

def last_zip_path() -> str:
    return os.path.join(PARTS_DIR, f"{FILE_BASE}.zip")

def iter_assembled_parts():
    """
    Concatena en stream: .z01..zNN + .zip final.
    Lanza FileNotFoundError si falta alguna parte (no tumba el server).
    """
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

    # stream: .z01..zNN
    for i in range(1, PART_COUNT + 1):
        p = part_path(i)
        with open(p, "rb", buffering=0) as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                yield chunk

    # stream: .zip final
    with open(lz, "rb", buffering=0) as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            yield chunk

def assemble_to_spooled():
    """Ensambla a un archivo temporal (RAM hasta 64MiB) y devuelve el handler reposicionado al inicio."""
    spooled = io.SpooledTemporaryFile(max_size=64 * 1024 * 1024)
    for chunk in iter_assembled_parts():
        spooled.write(chunk)
    spooled.seek(0)
    return spooled

def extract_to_dist():
    """Ensambla y descomprime en /dist."""
    dist_dir = os.path.join(PUBLIC_DIR, "dist")
    os.makedirs(dist_dir, exist_ok=True)
    with assemble_to_spooled() as fzip:
        with ZipFile(fzip) as zf:
            zf.extractall(dist_dir)
    return dist_dir

class Handler(SimpleHTTPRequestHandler):
    # Sirve archivos desde PUBLIC_DIR
    def translate_path(self, path):
        path = urlparse(path).path
        path = posixpath.normpath(path)
        words = [w for w in path.split('/') if w]
        out = PUBLIC_DIR
        for w in words:
            drive, w = os.path.splitdrive(w)
            head, w = os.path.split(w)
            if w in (os.curdir, os.pardir):
                continue
            out = os.path.join(out, w)
        return out

    def do_GET(self):
        if self.path.startswith("/download"):
            return self._handle_download()
        if self.path.startswith("/extract"):
            return self._handle_extract()
        if self.path.startswith("/health"):
            return self._ok_text("ok")
        if self.path in ("/", ""):
            self.path = "/index.html"
        return super().do_GET()

    def _ok_text(self, text, code=200):
        data = text.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _handle_download(self):
        """Concatena y transmite el ZIP completo como attachment (no crea archivo en disco)."""
        try:
            self.send_response(200)
            self.send_header("Content-Type", "application/zip")
            self.send_header("Content-Disposition", f'attachment; filename="{FILE_BASE}.zip"')
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            for chunk in iter_assembled_parts():
                self.wfile.write(chunk)
        except FileNotFoundError as e:
            self._ok_text(f"NO_ENCONTRADAS:\n{e}", code=404)
        except BrokenPipeError:
            # el cliente canceló la descarga
            pass
        except Exception as e:
            self._ok_text(f"ERROR: {e}", code=500)

    def _handle_extract(self):
        """Ensambla y descomprime en /dist, útil para verificar en Render."""
        try:
            dist = extract_to_dist()
            rel = os.path.relpath(dist, PUBLIC_DIR).replace("\\", "/")
            self._ok_text(f"EXTRAIDO_EN: /{rel}")
        except FileNotFoundError as e:
            self._ok_text(f"NO_ENCONTRADAS:\n{e}", code=404)
        except Exception as e:
            self._ok_text(f"ERROR: {e}", code=500)

def main():
    os.chdir(PUBLIC_DIR)
    httpd = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"Serving at http://0.0.0.0:{PORT}  (root: {PUBLIC_DIR})")
    print("Endpoints: /  /download  /extract  /health")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nBye!")

if __name__ == "__main__":
    if len(sys.argv) >= 2:
        PORT = int(sys.argv[1])
    main()
