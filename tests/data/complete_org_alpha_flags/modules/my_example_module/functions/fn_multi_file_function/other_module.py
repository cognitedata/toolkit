import re

import inflect


def to_camel(string: str, pluralize: bool = False, singularize: bool = False) -> str:
    """Convert snake_case_name to camelCaseName.

    Args:
        string: The string to convert.
        pluralize: Whether to pluralize the last word.
        singularize: Whether to singularize the last word.
    Returns:
        camelCase of the input string.

    Examples:
        >>> to_camel("a_b")
        'aB'
        >>> to_camel('camel_case', pluralize=True)
        'camelCases'
        >>> to_camel('best_directors', singularize=True)
        'bestDirector'
        >>> to_camel("ScenarioInstance_priceForecast")
        'scenarioInstancePriceForecast'
    """
    if "_" in string:
        # Could be a combination of snake and pascal/camel case
        parts = string.split("_")
        pascal_splits = [to_pascal(subpart) for part in parts for subpart in part.split("-") if subpart]
    elif "-" in string:
        # Could be a combination of kebab and pascal/camel case
        parts = string.split("-")
        pascal_splits = [to_pascal(subpart) for part in parts for subpart in part.split("_") if subpart]
    else:
        # Assume is pascal/camel case
        # Ensure pascal
        string = string[0].upper() + string[1:]
        pascal_splits = [string]
    string_split: list[str] = []
    for part in pascal_splits:
        # Split on capital letters to maintain the capital letters from the original string
        # The extra filter is to remove empty strings
        # This can happen if the string starts with a capital letter
        string_split.extend(sub for sub in re.split(r"(?=[A-Z])", part) if sub)

    if not string_split:
        string_split = [string]
    if pluralize and singularize:
        raise ValueError("Cannot pluralize and singularize at the same time")
    elif pluralize:
        string_split[-1] = as_plural(string_split[-1].casefold())
    elif singularize:
        string_split[-1] = as_singular(string_split[-1].casefold())
    try:
        return string_split[0].casefold() + "".join(word.capitalize() for word in string_split[1:])
    except IndexError:
        return ""


def to_pascal(string: str, pluralize: bool = False, singularize: bool = False) -> str:
    """Convert string to PascalCaseName.

    Args:
        string: The string to convert.
        pluralize: Whether to pluralize the last word.
        singularize: Whether to singularize the last word.
    Returns:
        PascalCase of the input string.

    Examples:
        >>> to_pascal("a_b")
        'AB'
        >>> to_pascal('camel_case', pluralize=True)
        'CamelCases'
        >>> to_pascal('best_directors', singularize=True)
        'BestDirector'
        >>> to_pascal("BestLeadingActress", singularize=True)
        'BestLeadingActress'
        >>> to_pascal("priceScenarios", pluralize=True)
        'PriceScenarios'
        >>> to_pascal("reserveScenarios", pluralize=True)
        'ReserveScenarios'
        >>> to_pascal("ScenarioInstance_priceForecast")
        'ScenarioInstancePriceForecast'
    """
    camel = to_camel(string, pluralize, singularize)
    return f"{camel[0].upper()}{camel[1:]}" if camel else ""


def to_snake(string: str, pluralize: bool = False, singularize: bool = False) -> str:
    """
    Convert input string to snake_case

    Args:
        string: The string to convert.
        pluralize: Whether to pluralize the last word.
        singularize: Whether to singularize the last word.
    Returns:
        snake_case of the input string.

    Examples:
        >>> to_snake("aB")
        'a_b'
        >>> to_snake('CamelCase')
        'camel_case'
        >>> to_snake('camelCamelCase')
        'camel_camel_case'
        >>> to_snake('Camel2Camel2Case')
        'camel_2_camel_2_case'
        >>> to_snake('getHTTPResponseCode')
        'get_http_response_code'
        >>> to_snake('get200HTTPResponseCode')
        'get_200_http_response_code'
        >>> to_snake('getHTTP200ResponseCode')
        'get_http_200_response_code'
        >>> to_snake('HTTPResponseCode')
        'http_response_code'
        >>> to_snake('ResponseHTTP')
        'response_http'
        >>> to_snake('ResponseHTTP2')
        'response_http_2'
        >>> to_snake('Fun?!awesome')
        'fun_awesome'
        >>> to_snake('Fun?!Awesome')
        'fun_awesome'
        >>> to_snake('10CoolDudes')
        '10_cool_dudes'
        >>> to_snake('20coolDudes')
        '20_cool_dudes'
        >>> to_snake('BestDirector', pluralize=True)
        'best_directors'
        >>> to_snake('BestDirectors', singularize=True)
        'best_director'
        >>> to_snake('BestLeadingActress', pluralize=True)
        'best_leading_actresses'
        >>> to_snake('APM_Activity', pluralize=True)
        'apm_activities'
        >>> to_snake('APM_Activities', singularize=True)
        'apm_activity'
        >>> to_snake('APM_Operation', pluralize=True)
        'apm_operations'
        >>> to_snake('APM_Asset', pluralize=True)
        'apm_assets'
        >>> to_snake('APM_Material', pluralize=True)
        'apm_materials'
    """
    pattern = re.compile(r"[A-Z]?[a-z]+|[A-Z]+(?=[A-Z][a-z]|\d|\W|$)|\d+")
    if "_" in string:
        words = [word for section in string.split("_") for word in pattern.findall(section)]
    else:
        words = pattern.findall(string)
    # Exception for 3D
    if len(words) > 1:
        if words[-2] == "3" and words[-1].lower() == "d":
            words[-2:] = [f"{words[-2]}{words[-1]}"]

    if pluralize and singularize:
        raise ValueError("Cannot pluralize and singularize at the same time")
    elif pluralize:
        words[-1] = as_plural(words[-1].casefold())
    elif singularize:
        words[-1] = as_singular(words[-1].casefold())
    return "_".join(map(str.lower, words))


class _Inflect:
    _engine: inflect.engine | None = None

    @classmethod
    def engine(cls) -> inflect.engine:
        if cls._engine is None:
            cls._engine = inflect.engine()
        return cls._engine


def as_plural(noun: str) -> str:
    """Pluralize a noun.

    Args:
        noun: The noun to pluralize.
    Returns:
        The pluralized noun.

    Examples:
        >>> as_plural('person')
        'persons'
        >>> as_plural('Roles')
        'Roles'
        >>> as_plural('activity')
        'activities'
    """
    numbers = re.findall(r"\d+$", noun)
    noun = noun[: -len(numbers[0])] if numbers else noun

    if noun and _Inflect.engine().singular_noun(noun) is False:
        noun = _Inflect.engine().plural_noun(noun)

    if numbers:
        return f"{noun}{numbers[0]}"
    else:
        return noun


def as_singular(noun: str) -> str:
    """Singularize a noun.

    Args:
        noun: The noun to singularize.
    Returns:
        The singularized noun.

    Examples:
        >>> as_singular('persons')
        'person'
        >>> as_singular('Roles')
        'Role'
        >>> as_singular('role')
        'role'
    """
    numbers = re.findall(r"\d+$", noun)
    noun = noun[: -len(numbers[0])] if numbers else noun
    if noun and isinstance(singular := _Inflect.engine().singular_noun(noun), str):
        noun = singular

    if numbers:
        return f"{noun}{numbers[0]}"
    else:
        return noun
