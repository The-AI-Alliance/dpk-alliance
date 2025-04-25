import argparse
from typing import Any


def str2bool(value: str) -> bool:
    """
    Convert string to boolean. Helper for getting boolean parameters
    :param value - input string
    """
    if value.strip().lower() in ("yes", "true", "t", "y", "1"):
        return True
    return False


class CLIArgumentProvider:
    """
    Interface for the implementation of the classes that populate parser
    with the required information and apply/validate user provided information
    """

    def __init__(self):
        self.params = {}

    @staticmethod
    def capture_parameters(
        args: argparse.Namespace, prefix: str, keep_prefix: bool = True
    ):
        """
        Converts a namespace of values into a dictionary of keys and values where the keys
        match the given prefix.
        :param args: namespace instance to read keys/values from
        :param prefix: optional prefix to restrict the set of namespace keys considered for inclusion in the
                       returned dictionary
        :param keep_prefix:  flag to keep prefix(true) in the keys in the resulting dictionary.
        :return:  a dictionary of keys matching the prefix and their values.  The keys in the dictionary may or may
                  not include the prefix.
        """
        captured = {}
        args_dict = vars(args)
        for key, value in args_dict.items():
            if prefix is None or key.startswith(prefix):
                if prefix is not None and not keep_prefix:
                    key = key.replace(prefix, "", 1)
                captured[key] = value
        return captured

    def add_input_params(self, parser: argparse.ArgumentParser) -> None:
        """
        Add arguments to the given parser.
        :param parser: parser
        :return:
        """
        pass

    def apply_input_params(self, args: argparse.Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments including at least, but perhaps more,
        arguments as defined by add_input_arguments().
        :return: True, if validate pass or False otherwise
        """
        return True

    def get_input_params(self) -> dict[str, Any]:
        """
        Provides a default implementation if the user has provided a set of keys to the initializer.
        These keys are used in apply_input_params() to extract our key/values from the global Namespace of args.
        :return:
        """
        return self.params


class ParamsUtils:
    """
    Class implementing support methods for parameters manipulation
    """

    @staticmethod
    def convert_to_ast(d: dict[str, Any]) -> str:
        """
        Converts dictionary to AST string
        :param d: dictionary
        :return: an AST string
        """
        ast_string = "{"
        first = True
        for key, value in d.items():
            if first:
                first = False
            else:
                ast_string += ", "
            if isinstance(value, str):
                ast_string += f"'{key}': '{value}'"
            else:
                ast_string += f"'{key}': {value}"
        ast_string += "}"
        return ast_string

    @staticmethod
    def dict_to_req(d: dict[str, Any], executor: str = "") -> list[str]:
        """
        Convert dictionary to a list of string parameters
        :param executor - executor name
        :param d: dictionary
        :return: an array of parameters
        """
        if executor != "":
            # local testing
            res = [executor]
        else:
            # remote invoke
            res = [f"python {executor}"]
        for key, value in d.items():
            res.append(f"--{key}={value}")
        return res

    @staticmethod
    def __dict_to_str(
        dict_val: dict[str, str],
        initial_indent: str,
        indent_per_level: str,
        as_value: bool,
    ) -> str:
        all_text = ""
        if as_value:
            all_text = all_text + "{ "
        first = True
        last_line = ""
        for key, value in dict_val.items():
            if isinstance(value, dict):
                text = ParamsUtils.__dict_to_str(
                    value, initial_indent + indent_per_level, indent_per_level, as_value
                )
            else:
                if as_value:
                    key = "'" + key + "'"
                    if isinstance(value, str):
                        value = "'" + value + "'"
                text = initial_indent + key + ": " + str(value)
            if first:
                new_text = ""
            elif as_value:
                new_text = ", "
            else:
                new_text = "\n"
            if as_value and len(last_line) + len(text) > 60:
                new_text = new_text + "\n"
                last_line = ""
            new_text = new_text + text
            all_text = all_text + new_text
            last_line = last_line + new_text
            first = False
        all_text = all_text.strip()
        if as_value:
            all_text = all_text + " }"
        return all_text

    @staticmethod
    def get_config_parameter(params: dict[str, Any], cli_prefix: str = "data_") -> str:
        """
        Get the key name of the config parameter
        :param params: original parameters
        :param cli_prefix: cli prefix
        :return: the name of the key for the config parameter
        """
        # find config parameter
        config = None
        print(params)
        for key in params.keys():
            if key.startswith(cli_prefix) and key.endswith("config"):
                if (
                    params[key] is not None
                    and params[key] != "None"
                    and len(params[key]) > 0
                ):
                    config = key
                    break
        return config

    @staticmethod
    def get_ast_help_and_example_text(
        help_dict: dict[str, str], examples: list[dict[str, Any]]
    ):
        initial_indent = ""
        indent_per_level = "   "
        help_txt = ParamsUtils.__dict_to_str(
            help_dict, initial_indent, indent_per_level, False
        )
        if examples is not None:
            example_txt = "\n" + initial_indent
            if len(examples) == 1:
                example_txt += "Example: "
            else:
                example_txt += "Example(s):"
            for example_dict in examples:
                e_txt = ParamsUtils.__dict_to_str(
                    example_dict, initial_indent, indent_per_level, True
                )
                if len(examples) == 1:
                    example_txt = example_txt + e_txt
                else:
                    example_txt = example_txt + "\n" + initial_indent + "    " + e_txt
        else:
            example_txt = ""
        msg = help_txt + example_txt
        return msg

    @staticmethod
    def get_ast_help_text(help_example_dict: dict[str, list[str]]):
        """
        Create some help text for an AST-formatted parameter value.
        :param help_example_dict:  This dictionary of lists, where they keys
        correspond to the parameter names and the list is a pair of values.
        The 1st value in the list is an example value for the option, the 2nd in is the help text.
        If you need to provide more than 1 example, use get_ast_help_and_example_text() which
        allows a list of examples.
        Example:
            help_example_dict = {
                'access_key': ["access", 'access key help text'],
                'secret_key': ["secret", 'secret key help text'],
                'url': ["https://s3.us-east.cloud-object-storage.appdomain.cloud", "s3 url"],
            }
            parser.add_argument(
                "--s3_cred",
                type=ast.literal_eval,
                default=None,
                help="ast string of options for s3 credentials\n" +
                     ParamsUtils.get_ast_help_text(help_example_dict)
            )
        :return:  a string to be included in help text, usually concatenated with the general
        parameter help text.
        """

        help_dict = {}
        example_dict = {}
        for key, value in help_example_dict.items():
            if not isinstance(value, list):
                raise ValueError("key value for key " + key + " is not a list")
            if len(value) != 2:
                raise ValueError("List for key " + key + " is not a list of length 2")
            example_value = value[0]
            help_text = value[1]
            help_dict[key] = help_text
            example_dict[key] = example_value
        return ParamsUtils.get_ast_help_and_example_text(help_dict, [example_dict])
