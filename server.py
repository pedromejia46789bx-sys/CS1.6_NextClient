import os, json
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, unquote

PORT = int(os.getenv("PORT", "8080"))
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Carpeta donde viven manifest.json y las partes
PARTS_DIR = os.path.join(BASE_DIR, os.getenv("PARTS_DIR", "files"))
# (compat) si quieres forzar un nombre de descarga distinto al del manifest:
DOWNLOAD_NAME_OVERRIDE = os.getenv("DOWNLOAD_FILE")  # opcional

class Handler(SimpleHTTPRequestHandler):
    def translate_path(self, path):
        path = urlparse(path).path
        path = unquote(path)
        if path in ("/", ""):
            path = "/index.html"
        return os.path.join(BASE_DIR, path.lstrip("/"))

    def list_directory(self, path):
        self.send_error(403, "Directory listing disabled")
        return None

    def do_GET(self):
        p = urlparse(self.path).path
        if p == "/diag":
            return self._diag()
        if p == "/where":
            return self._where()
        if p == "/download":
            return self._send_download_streaming()
        return super().do_GET()

    # -------- utilidades --------
    def _load_manifest(self):
        man_path = os.path.join(PARTS_DIR, "manifest.json")
        if not os.path.exists(man_path):
            return None, f"manifest.json no encontrado en {PARTS_DIR}"
        try:
            with open(man_path, "r", encoding="utf-8") as mf:
                manifest = json.load(mf)
            if "filename" not in manifest or "parts" not in manifest:
                return None, "manifest.json inválido: falta 'filename' o 'parts'"
            if not isinstance(manifest["parts"], list) or not manifest["parts"]:
                return None, "manifest.json inválido: 'parts' vacío"
            return manifest, None
        except Exception as e:
            return None, f"Error leyendo manifest.json: {e}"

    def _iter_parts(self, manifest):
        """
        Itera (ruta_absoluta, size_declarado) en el orden declarado por manifest["parts"].
        Cada item de 'parts' debe tener al menos 'path'; 'size' es opcional.
        """
        for entry in manifest["parts"]:
            rel = entry.get("path")
            if not rel:
                raise FileNotFoundError("Entrada de parte sin 'path' en manifest")
            abs_path = os.path.join(PARTS_DIR, rel)
            if not os.path.exists(abs_path):
                raise FileNotFoundError(f"Parte no encontrada: {rel}")
            declared_size = entry.get("size")
            yield abs_path, declared_size

    def _total_length_from_manifest(self, manifest):
        total = 0
        complete = True
        for entry in manifest.get("parts", []):
            s = entry.get("size")
            if isinstance(s, int):
                total += s
            else:
                complete = False
                break
        return total if complete else None

    # -------- endpoints --------
    def _send_download_streaming(self):
        # 1) Cargar manifest
        manifest, err = self._load_manifest()
        if err:
            self.send_error(404, err)
            return

        filename = DOWNLOAD_NAME_OVERRIDE or manifest.get("filename", "download.bin")
        mime = manifest.get("mime", "application/octet-stream")

        # 2) Calcular Content-Length si posible (suma sizes del manifest)
        content_length = self._total_length_from_manifest(manifest)

        # 3) Responder headers
        self.send_response(200)
        self.send_header("Content-Type", mime)
        if content_length is not None:
            self.send_header("Content-Length", str(content_length))
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.end_headers()

        # 4) Enviar concatenación de partes en bloques
        try:
            for abs_path, _declared in self._iter_parts(manifest):
                with open(abs_path, "rb") as f:
                    while True:
                        chunk = f.read(64 * 1024)
                        if not chunk:
                            break
                        self.wfile.write(chunk)
        except FileNotFoundError as e:
            # Si alguna parte falta, devolvemos 500 después de headers
            # (el cliente verá descarga interrumpida)
            # Para depurar fácil:
            try:
                self.wfile.write(f"\nERROR: {e}".encode("utf-8"))
            except Exception:
                pass

    def _diag(self):
        def tree(root):
            lines = []
            for dirpath, dirnames, filenames in os.walk(root):
                rel = os.path.relpath(dirpath, root)
                rel = "." if rel == "." else rel
                lines.append(f"[{rel}]")
                for d in sorted(dirnames):
                    lines.append(f"  <DIR> {d}")
                for f in sorted(filenames):
                    lines.append(f"       {f}")
            return "\n".join(lines)

        manifest, err = self._load_manifest()
        man_info = "NO MANIFEST" if err else json.dumps(
            {"filename": manifest.get("filename"),
             "mime": manifest.get("mime"),
             "parts_count": len(manifest.get("parts", []))},
            ensure_ascii=False, indent=2
        )

        msg = (
            f"BASE_DIR: {BASE_DIR}\n"
            f"PARTS_DIR: {PARTS_DIR}\n"
            f"PORT: {PORT}\n\n"
            f"Manifest: {man_info}\n\n"
            f"Estructura de archivos (BASE_DIR):\n{tree(BASE_DIR)}\n"
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(msg)))
        self.end_headers()
        self.wfile.write(msg)

    def _where(self):
        man_path = os.path.join(PARTS_DIR, "manifest.json")
        info = f"BASE_DIR={BASE_DIR}\nPARTS_DIR={PARTS_DIR}\nMANIFEST={man_path}\n"
        data = info.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

def main():
    os.chdir(BASE_DIR)
    httpd = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[OK] Server en http://localhost:{PORT}/")
    print("  /          -> index.html")
    print("  /download  -> reconstruye y descarga desde manifest+partes")
    print("  /diag      -> diagnóstico (árbol de archivos y manifest)")
    print("  /where     -> rutas internas (BASE_DIR, PARTS_DIR, manifest)")
    httpd.serve_forever()

if __name__ == "__main__":
    main()
