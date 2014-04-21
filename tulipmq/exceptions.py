
class TulipMQException(Exception):
    """ """


class TMQNoNamespaceHandler(Exception):
    """There is no namespace class for handling such job tupe"""


class TMQNoActionMethod(Exception):
    """Namespace handler does not contain specified action"""
