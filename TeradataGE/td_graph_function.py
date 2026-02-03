import teradataml as tdml
import pandas as pd
import numpy as np

from teradataml.common import messages
from teradataml.common.constants import TeradataConstants, ValibConstants as VC
from teradataml.common.exceptions import TeradataMlException
from teradataml.common.messages import Messages, MessageCodes

from GraphProject import configure

class td_graph_object:
    def __init__(self,
                 edge_table_name,
                 edge_from_node_column_name,
                 edge_to_node_column_name,
                 edge_weight_column_name = None,
                 edge_attributes = None,
                 node_table_name = None,
                 node_id_column_name = None,
                 node_label_column_name = None,
                 source_id = None,
                 target_id = None
                ):

        if configure.graph_install_location is None:
            message = Messages.get_message(MessageCodes.UNKNOWN_INSTALL_LOCATION,
                                           "Graph Analytics",
                                           "option 'configure.graph_install_location'")
            raise TeradataMlException(message, MessageCodes.MISSING_ARGS)
        else:
            self.graphdb = configure.graph_install_location

        self.edge_table_name = edge_table_name
        self.edge_from_node_column_name = edge_from_node_column_name
        self.edge_to_node_column_name = edge_to_node_column_name
        self.edge_weight_column_name = edge_weight_column_name
        self.edge_attributes = edge_attributes
        self.node_table_name = node_table_name
        self.node_id_column_name = node_id_column_name
        self.node_label_column_name = node_label_column_name
        self.source_id = source_id
        self.target_id = target_id
        self.topology_path_result_table = None
        self.topology_path_result_column = None
        self.shortpath_path_result_table = None
        self.max_path_length = 100

    # Displace Edge definition and record
    def edge_info(self):
        if self.edge_table_name is None:
            raise ValueError("Missing Edge Table Name (edge_table_name)")
        if self.edge_from_node_column_name is None:
            raise ValueError("Missing Edge FROM Column name (edge_from_node_column_name)")
        if self.edge_to_node_column_name is None:
            raise ValueError("Missing Edge TO Column name (edge_to_node_column_name)")

        # Get edge count from edge table
        SQL = f"SELECT COUNT(0) AS reccnt FROM {self.edge_table_name}"
        df = tdml.DataFrame.from_query(SQL).to_pandas()
        rec_cnt = int(df.iloc[0,0])

        # Get edge count from edge table
        SQL = f"""SELECT count(distinct node_id) AS rec_cnt FROM (
                  SELECT distinct {self.edge_from_node_column_name} as node_id from {self.edge_table_name}
                  UNION ALL 
                  SELECT distinct {self.edge_to_node_column_name} as node_id from {self.edge_table_name}) t"""
        df = tdml.DataFrame.from_query(SQL).to_pandas()
        node_cnt = int(df.iloc[0,0])

        if self.edge_weight_column_name is None:
            info = f"Edge Table [{self.edge_table_name}]: From[{self.edge_from_node_column_name}] To[{self.edge_to_node_column_name}] with No Weight. # of edges = {rec_cnt} & # of nodes used = {node_cnt}."
        else:
            info = f"Edge Table [{self.edge_table_name}]: From[{self.edge_from_node_column_name}] To[{self.edge_to_node_column_name}] with Weight from [{self.edge_weight_column_name}]. # of edges = {rec_cnt} & # of nodes used = {node_cnt}."

        return info
        
    # Displace Edge definition and record
    def node_info(self):
        if self.node_table_name is None:
            raise ValueError("Missing Node Table Name (node_table_name)")
        if self.node_id_column_name is None:
            raise ValueError("Missing Node column Name (node_id_column_name)")

        # Get unique node count from node table
        SQL = f"SELECT COUNT(0) AS rec_cnt FROM {self.node_table_name}"
        df = tdml.DataFrame.from_query(SQL).to_pandas()
        rec_cnt = int(df.iloc[0,0])

        return f"Node Table [{self.node_table_name}]: Node ID [{self.node_id_column_name}]. # of Nodes = {rec_cnt}."


    # Get and set the from node id from label for source id
    def set_node_id_from_label_source(self, label):
        if self.node_table_name is None:
            raise ValueError("Missing Node Table Name (node_table_name)")
        if self.node_id_column_name is None:
            raise ValueError("Missing Node column Name (node_id_column_name)")
        if self.node_label_column_name is None:
            raise ValueError("Missing Node Label Column Name (node_label_column_name)")
        SQL = f"""SELECT {self.node_id_column_name} AS node_id 
                  FROM {self.node_table_name}
                  WHERE {self.node_label_column_name} Like '%{label}%'"""
        df = tdml.DataFrame.from_query(SQL).to_pandas().reset_index()
        self.source_id = int(df.iloc[0,0])
        return self.source_id


    # Get and set the from node id from label for target id
    def set_node_id_from_label_target(self, label):
        if self.node_table_name is None:
            raise ValueError("Missing Node Table Name (node_table_name)")
        if self.node_id_column_name is None:
            raise ValueError("Missing Node column Name (node_id_column_name)")
        if self.node_label_column_name is None:
            raise ValueError("Missing Node Label Column Name (node_label_column_name)")
        SQL = f"""SELECT {self.node_id_column_name} AS node_id 
                  FROM {self.node_table_name}
                  WHERE {self.node_label_column_name} Like '%{label}%'"""
        df = tdml.DataFrame.from_query(SQL).to_pandas().reset_index()
        self.target_id = int(df.iloc[0,0])
        return self.target_id


    def td_topology(self, source_id = None, max_path_length=100, return_data = 'P', output_table = None):
        if self.edge_table_name is None:
            raise ValueError("Missing Edge Table Name (edge_table_name)")
        if self.edge_from_node_column_name is None:
            raise ValueError("Missing Edge FROM Column name (edge_from_node_column_name)")
        if self.edge_to_node_column_name is None:
            raise ValueError("Missing Edge TO Column name (edge_to_node_column_name)")
            
        if source_id is None:
            if self.source_id is None:
                raise ValueError("Source Node ID is not defined!!!")
            else:
                cur_source_id = self.source_id
        else:
            self.source_id = source_id
            cur_source_id = source_id
        if self.edge_weight_column_name is None or self.edge_weight_column_name=="" :
            weight_column_adj = "NULL"
        else:
            weight_column_adj = f"'{self.edge_weight_column_name}'"

        if return_data.upper() not in ['P','N']:
            raise ValueError("return_data must be P(path) or N(node) only!!!")

        if output_table is None or output_table=="" :
            output_table_adj = "NULL"
        else:
            output_table_adj = f"'{output_table}'"
            self.topology_path_result_table = output_table

        self.max_path_length = max_path_length

        SQL = f"""CALL {self.graphdb}.graph_topology_sp('{self.edge_table_name}',
                                                        '{self.edge_from_node_column_name}',
                                                        '{self.edge_to_node_column_name}', 
                                                         {weight_column_adj}, 
                                                         {cur_source_id}, 
                                                         {max_path_length},
                                                        '{return_data.upper()}',
                                                         {output_table_adj}
                                                        );"""
        result = tdml.execute_sql(SQL)
        rows0 = result.fetchall()
        result.nextset()
        rows1 = result.fetchall()
    
        # All returned paths in dataframe, 1 row per path
        if return_data.upper() == 'P':
            local_df = pd.DataFrame(rows1, columns=["fullpath","weight"])
            self.topology_path_result_column = "fullpath"
        else:
            local_df = pd.DataFrame(rows1, columns=["Node","weight"])
            self.topology_path_result_column = "node"
    
        return(local_df)


    # Caculate the shortpath from source_id to target_id
    def td_shortest_path(self, source_id = None, target_id = None, max_path_length=100, output_table = None ):
        if self.edge_table_name is None:
            raise ValueError("Missing Edge Table Name (edge_table_name)")
        if self.edge_from_node_column_name is None:
            raise ValueError("Missing Edge FROM Column name (edge_from_node_column_name)")
        if self.edge_to_node_column_name is None:
            raise ValueError("Missing Edge TO Column name (edge_to_node_column_name)")

        # check which source id to use
        if source_id is None:
            if self.source_id is None:
                raise ValueError("Source Node ID is not defined!!!")
            else:
                cur_source_id = self.source_id
        else:
            self.source_id = source_id
            cur_source_id = source_id

        # check which target id to use
        if target_id is None:
            if self.target_id is None:
                raise ValueError("Target Node ID is not defined!!!")
            else:
                cur_target_id = self.target_id
        else:
            self.target_id = target_id
            cur_target_id = target_id


        if self.edge_weight_column_name is None or self.edge_weight_column_name=="" :
            weight_column_adj = "NULL"
        else:
            weight_column_adj = f"'{self.edge_weight_column_name}'"

        if output_table is None or output_table=="" :
            output_table_adj = "NULL"
        else:
            output_table_adj = f"'{output_table}'"
            self.shortpath_path_result_table = output_table

        self.max_path_length = max_path_length

        SQL = f"""CALL {self.graphdb}.graph_shortest_path_sp('{self.edge_table_name}',
                                                             '{self.edge_from_node_column_name}',
                                                             '{self.edge_to_node_column_name}', 
                                                              {weight_column_adj}, 
                                                              {cur_source_id}, 
                                                              {cur_target_id}, 
                                                              {max_path_length},
                                                              {output_table_adj}
                                                             );"""
        result = tdml.execute_sql(SQL)
        rows0 = result.fetchall()
        result.nextset()
        rows1 = result.fetchall()
    
        # All returned paths in dataframe, 1 row per path
        local_df = pd.DataFrame(rows1, columns=["fullpath","weight"])
    
        return(local_df)


    def td_graph_path_decode(self, input_table, edge_attributes=None, output_table = None):
        if self.edge_table_name is None:
            raise ValueError("Missing Edge Table Name (edge_table_name)")
        if self.edge_from_node_column_name is None:
            raise ValueError("Missing Edge FROM Column name (edge_from_node_column_name)")
        if self.edge_to_node_column_name is None:
            raise ValueError("Missing Edge TO Column name (edge_to_node_column_name)")
        if self.node_table_name is None:
            raise ValueError("Missing Node Table Name (node_table_name)")
        if self.node_id_column_name is None:
            raise ValueError("Missing Node column Name (node_id_column_name)")
        if self.node_label_column_name is None:
            raise ValueError("Missing Node Label Column Name (node_label_column_name)")
        if input_table is None:
            raise ValueError("Missing tablename with pathname (input_table)")

        if self.topology_path_result_table is None or self.topology_path_result_column is None:
            raise ValueError("Missing topology_path_result_table, please run td_topology function with output table!!!")

        if edge_attributes is None:
            if self.edge_attributes is None:
                raise ValueError("Missing Edge Attribute column(s) (edge_attributes)!!!")
        else:
            if isinstance(edge_attributes, list):
                self.edge_attributes = edge_attributes
            else:
                self.edge_attributes = [edge_attributes]

        if output_table is not None:
            self.topology_path_decode_table = output_table
            output_table_adj = f'{output_table}'
        else:
            output_table_adj = 'NULL'

        SQL = f"""CALL {self.graphdb}.graph_path_decode_sp('{input_table}',
                                                           '{self.topology_path_result_column}',
                                                           '{self.edge_table_name}',
                                                           '{self.edge_from_node_column_name}',
                                                           '{self.edge_to_node_column_name}',
                                                           '{"|".join(self.edge_attributes)}',
                                                           '{self.node_table_name}',
                                                           '{self.node_id_column_name}',
                                                           '{self.node_label_column_name}',
                                                           {output_table_adj})"""

        result = tdml.execute_sql(SQL)
        rows0 = result.fetchall()
        result.nextset()
        rows1 = result.fetchall()
        local_df = pd.DataFrame(rows1, columns=["token_no","from_id", "to_id","n1_label"] + self.edge_attributes + ["n2_label"] )
        return(local_df)

