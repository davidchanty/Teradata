import os
from teradataml.common.exceptions import TeradataMlException
from teradataml.common.messages import Messages
from teradataml.common.messagecodes import MessageCodes

class _ConfigureSuper(object):

    def __init__(self):
        pass

    def _SetKeyValue(self, name, value):
        super().__setattr__(name, value)

    def _GetValue(self, name):
        return super().__getattribute__(name)

def _create_property(name):
    storage_name = '_' + name

    @property
    def prop(self):
        return self._GetValue(storage_name)

    @prop.setter
    def prop(self, value):
        self._SetKeyValue(storage_name, value)

    return prop


class _Configure(_ConfigureSuper):
    """
    Options to configure database related values.
    """

    graph_install_location = _create_property('graph_install_location')
    temp_table_database = _create_property('temp_table_database')
    temp_view_database = _create_property('temp_view_database')

    def __init__(self, 
                 graph_install_location=None,
                 temp_table_database=None,
                 temp_view_database=None
                ):

        """
        PARAMETERS:

            graph_install_location:
                Specifies the name of the database where Graph Analytics functions
                are installed.
                Types: string
                Example:
                    # Set the Graph Analytics functions install location to 'GraphDB'
                    # when VAL functions are installed in 'SYSLIB'.
                    GraphProject.configure.graph_install_location = "GraphDB"

        """
        super().__init__()
        super().__setattr__('graph_install_location', graph_install_location)
        super().__setattr__('temp_table_database', temp_table_database)
        super().__setattr__('temp_view_database', temp_view_database)

        # internal configurations
        # These configurations are internal and should not be
        # exported to the user's namespace.



    def __setattr__(self, name, value):
        if hasattr(self, name):
            if name in ['graph_install_location']:
                if not isinstance(value, str):
                    raise TeradataMlException(Messages.get_message(MessageCodes.UNSUPPORTED_DATATYPE, name,
                                                                   'str'),
                                              MessageCodes.UNSUPPORTED_DATATYPE)
                if name == 'local_storage':
                    # Validate if path exists.
                    if not os.path.exists(value):
                        raise TeradataMlException(
                            Messages.get_message(MessageCodes.PATH_NOT_FOUND).format(value),
                            MessageCodes.PATH_NOT_FOUND)

            elif name in ['temp_table_database', 'temp_view_database']:
                if not isinstance(value, str) and not isinstance(value, type(None)):
                    raise TeradataMlException(Messages.get_message(MessageCodes.UNSUPPORTED_DATATYPE, name,
                                                                   'str or None'),
                                              MessageCodes.UNSUPPORTED_DATATYPE)
 
            super().__setattr__(name, value)
        else:
            raise AttributeError("'{}' object has no attribute '{}'".format(self.__class__.__name__, name))

    def __get_temp_object_type(self, value):
        """
        Get the temporary object type based on the value provided.
        Default behavior is to create views that will be garbage collected at the end.
        """
        from teradataml.common.constants import TeradataConstants
        if value and value.upper() == "VT":
            return TeradataConstants.TERADATA_VOLATILE_TABLE
        # This we will need in the future.
        # elif value and value.upper() in ["TT", "PT"]:
        #     return TeradataConstants.TERADATA_TABLE
        return TeradataConstants.TERADATA_VIEW

    def __validate_db_tbl_attrs(self, name, value):
        if not isinstance(value, str) and not isinstance(value, type(None)):
            raise TypeError(Messages.get_message(MessageCodes.UNSUPPORTED_DATATYPE, name,
                                                 'str or None'))


configure = _Configure()