import json

from cognite.client.data_classes.data_modeling import View, Container, DataModel
from pathlib import Path
import re
import yaml
ROOT = Path(__file__).parent

JSON_PATTERN = re.compile(r"^(\w+\.)?(container|view|datamodel)\.json$")


def main():
    for file in (ROOT / "modules").glob("**/*.json"):
        if not(match := JSON_PATTERN.match(file.name)):
            continue
        type_ = match.group(2)
        data = json.loads(file.read_text())
        if type_ == "container":
            items =  Container.load(data)
        elif type_ == "view":
            items = View.load(data)
        elif type_ == "datamodel":
            items = DataModel.load(data)
        else:
            raise NotImplementedError

        yaml_file = file.parent / f"{match.group(1)}{type_}.yaml"
        yaml_file.write_text(yaml.dump(items.as_apply().dump(camel_case=True)))
        print("Converted", file, "to", yaml_file)


if __name__ == '__main__':
    main()
