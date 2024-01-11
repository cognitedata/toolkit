from constants import REPO_ROOT

from cognite_toolkit.cdf_tk.templates import COGNITE_MODULES


def main():
    yaml_files = list((REPO_ROOT / "cognite_toolkit" / COGNITE_MODULES).glob("**/*.yaml"))

    for yaml_file in yaml_files:
        byte_content = yaml_file.read_bytes()
        yaml_file.write_bytes(byte_content.replace(b"\r\n", b"\n"))


if __name__ == "__main__":
    main()
