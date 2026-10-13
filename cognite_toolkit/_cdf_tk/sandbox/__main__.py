import argparse

from cognite_toolkit._cdf_tk.sandbox._server import make_server


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the modeling sandbox HTTP server.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    server = make_server(args.host, args.port)
    print(f"Sandbox server listening on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
