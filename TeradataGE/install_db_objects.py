import os
import pandas as pd
import teradataml as tdml

from teradataml.common import messages
from teradataml.common.constants import TeradataConstants, ValibConstants as VC
from teradataml.common.exceptions import TeradataMlException
from teradataml.common.messages import Messages, MessageCodes

from GraphProject import configure

def install_graph_functions():

    if configure.graph_install_location is None:
        message = Messages.get_message(MessageCodes.UNKNOWN_INSTALL_LOCATION,
                                       "Graph Analytics",
                                       "option 'configure.graph_install_location'")
        raise TeradataMlException(message, MessageCodes.MISSING_ARGS)
    else:
        graphdb = configure.graph_install_location

    sp_template_location = f'{os.path.dirname(configure.__file__)}\\SP'

    for sp_name in ['drop_vt_sp', 'graph_shortest_path_sp', 'graph_topology_sp', 'graph_path_decode_sp']:
        print(f"Installing {sp_name}...")
        with open(f'{sp_template_location}/{sp_name}.sql', 'r') as f:
            content = f.read()
        content = content.replace('[install_database]', graphdb)
        tdml.execute_sql(content)
    print("Done installing SPs.")

    result = tdml.execute_sql(f"HELP DATABASE {graphdb}")
    rows0 = result.fetchall()
    result_df = pd.DataFrame(rows0).iloc[:,:5]
    result_df.columns = ["SP Name","Kind","Comment","Protection","CreatorName"]
    result_df = result_df[result_df.Kind=='P ']
    return(result_df)
