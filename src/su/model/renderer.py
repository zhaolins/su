class InvalidValueError(BaseException):
    pass


class ResultManipulator:
    def __init__(self, v, **options):
        self.v = v
        self.options = options

    def __call__(self, data, key):
        raise NotImplementedError


class Invisible(ResultManipulator):
    def __call__(self, data, key):
        data.pop(key, None)
        return data


class Rename(ResultManipulator):
    def __call__(self, data, key):
        data[self.options['value']] = data.pop(key, None)
        return data


def render(data, key, render_fn, **options):
    if key not in data:
        return data

    value = render_fn(data[key], **options)
    if hasattr(value, '__class__') and issubclass(value.__class__, ResultManipulator):
        data = value(data, key)
    else:
        data[key] = value
    return data


def RELATIVES(v, **options):
    if v.data is not None and hasattr(v, 'renderer'):
        return v.renderer(v.data, **options)
    else:
        return None

def ENTITY(v, **options):
    scenarios = options.get('scenarios', set())
    roles = options.get('roles', set())
    if v:
        return v.render(scenarios, roles)
    else:
        return None

def ENTITIES(v, **options):
    scenarios = options.get('scenarios', set())
    roles = options.get('roles', set())
    results = []
    if isinstance(v, list):
        for entity in v:
            if hasattr(entity, 'render'):
                results.append(entity.render(scenarios, roles))
    elif hasattr(v, 'render'):
        results = v.render(scenarios, roles)
    return results


def INT(v, **options):
    try:
        result = int(v)
        return result
    except (TypeError, ValueError):
        raise InvalidValueError(v)


def FLOAT(v, **options):
    try:
        result = float(v)
        return result
    except (TypeError, ValueError):
        raise InvalidValueError(v)


def STRING(v, **options):
    try:
        result = str(v)
        return result
    except (TypeError, ValueError):
        raise InvalidValueError(v)


def LIST(v, **options):
    try:
        result = [str(val) for val in v]
        return result
    except (TypeError, ValueError):
        raise InvalidValueError(v)


def INTLIST(v, **options):
    try:
        result = [int(val) for val in v]
        return result
    except (TypeError, ValueError):
        raise InvalidValueError(v)


def INVISIBLE(v, **options):
    return Invisible(v)


def RENAME(v, **options):
    return Rename(v, **options)


def DATETIME(v, **options):
    f = options.pop('format', '%Y-%m-%d %H:%M:%S')
    try:
        result = v.strftime(f)
        return result
    except (AttributeError, TypeError, ValueError):
        raise InvalidValueError(v)