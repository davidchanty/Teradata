import teradataml as tdml
import pandas as pd
import numpy as np
import plotly.graph_objects as go

from teradataml.common import messages
from teradataml.common.constants import TeradataConstants, ValibConstants as VC
from teradataml.common.exceptions import TeradataMlException
from teradataml.common.messages import Messages, MessageCodes
from teradataml.context.context import _get_user

from TeradataGE import configure



####################################
# Start of td_graph_function Class #
####################################
class td_graph_object:
    """
    TeradataGE class to store the definition for the graph object, including Edge and Node information

    Attributes:
        edge_table_name (str): Table/view name for EDGE
        edge_from_node_column_name (str): Column name for the FROM node id in edge table
        edge_to_node_column_name (str): Column name for the TO node id in edge table
        edge_type_column_name (str): [Optional] Column name for Edge Type. Default is not edge type
        edge_weight_column_name (str): [Optional] Column name for weight/probability. Default is no weight
        datetime_column_name (str): [Optional] Date or Time stamp column for temporal process. Default is no. If it's defined, only the next edge is the date or time greater than previous edge
        weight_type (str): [Optional] If weight column is defined, weight type should be W(for weight) or P(for probability).
        edge_attributes (str): [Optional] List of attributes column(s) for edge
        node_table_name (str): [Optional] Table/view name for NODE
        node_id_column_name (str): [Optional] node id column name
        node_type_column_name (str): [Optional] node type
        node_label_column_name (str): [Optional] Node name label for display
        source_id (integer): [Optional] List of starting node id (can be added later)
        target_id (integer): [Optional] List of targeting node id (can be added later)

    Example:
        td_graph_obj = td_graph_function.td_graph_object(etc...)

    """
    def __init__(self,
                 edge_table_name,
                 edge_from_node_column_name,
                 edge_to_node_column_name,
                 edge_type_column_name = None,
                 edge_weight_column_name = None,
                 datetime_column_name = None,
                 weight_type = 'W',
                 edge_attributes = None,
                 node_table_name = None,
                 node_id_column_name = None,
                 node_type_column_name = None,
                 node_label_column_name = None,
                 database_name = None,
                 source_table_name = None,
                 source_node_name = None,
                 target_id = None,
                 pr_table_name = None,
                 pr_node_name = None,
                 pr_score_name = None
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
        self.edge_type_column_name = edge_type_column_name
        self.edge_weight_column_name = edge_weight_column_name
        self.datetime_column_name = datetime_column_name
        self.weight_type = weight_type
        self.edge_attributes = edge_attributes
        self.node_table_name = node_table_name
        self.node_id_column_name = node_id_column_name
        self.node_type_column_name = node_type_column_name
        self.node_label_column_name = node_label_column_name
        self.source_table_name = source_table_name
        self.source_node_name = source_node_name
        self.target_id = target_id
        self.topology_path_result_table = None
        self.topology_path_result_column = None
        self.shortpath_path_result_table = None
        self.pr_table_name = None
        self.pr_node_name = None
        self.pr_score_name = None
        self.max_path_length = 100
        self.edge_type_list = None
        if database_name is None:
            self.database_name = tdml.execute_sql("SELECT database").fetchone()[0]
        else:
            self.database_name = database_name
        self.last_outputtable = None
        self.last_outputtable_kind = None

    #######################################
    # Displace Edge definition and record #
    #######################################
    def edge_info(self):
        if self.edge_table_name is None:
            raise ValueError("Missing Edge Table Name (edge_table_name)")
        if self.edge_from_node_column_name is None:
            raise ValueError("Missing Edge FROM Column name (edge_from_node_column_name)")
        if self.edge_to_node_column_name is None:
            raise ValueError("Missing Edge TO Column name (edge_to_node_column_name)")

        # Get edge count from edge table
        SQL = f"SELECT COUNT(0) AS reccnt FROM {self.database_name}.{self.edge_table_name}"
        df = tdml.DataFrame.from_query(SQL).to_pandas()
        rec_cnt = int(df.iloc[0,0])

        # Get edge count from edge table
        SQL = f"""SELECT count(distinct node_id) AS rec_cnt FROM (
                  SELECT distinct {self.edge_from_node_column_name} as node_id from {self.database_name}.{self.edge_table_name}
                  UNION ALL 
                  SELECT distinct {self.edge_to_node_column_name} as node_id from {self.database_name}.{self.edge_table_name}) t"""
        df = tdml.DataFrame.from_query(SQL).to_pandas()
        node_cnt = int(df.iloc[0,0])

        if self.edge_weight_column_name is None:
            info = f"Edge Table [{self.edge_table_name}]: From[{self.edge_from_node_column_name}] To[{self.edge_to_node_column_name}] with No Weight. # of edges = {rec_cnt} & # of nodes used = {node_cnt}."
        else:
            info = f"Edge Table [{self.edge_table_name}]: From[{self.edge_from_node_column_name}] To[{self.edge_to_node_column_name}] with Weight from [{self.edge_weight_column_name}]. # of edges = {rec_cnt} & # of nodes used = {node_cnt}."

        if self.edge_type_column_name is not None:
            SQL = f"SELECT DISTINCT {self.edge_type_column_name} AS edge_type FROM {self.database_name}.{self.edge_table_name}"
            df = tdml.DataFrame.from_query(SQL).to_pandas()
            self.edge_type_list = df['edge_type'].tolist()
            info += f"Possible Edge Type = [{','.join(self.edge_type_list)}]" 
        else:
            self.edge_type_list = None

        return info

        
    #######################################
    # Displace Edge definition and record #
    #######################################
    def node_info(self):
        if self.node_table_name is None:
            raise ValueError("Missing Node Table Name (node_table_name)")
        if self.node_id_column_name is None:
            raise ValueError("Missing Node column Name (node_id_column_name)")

        # Get unique node count from node table
        SQL = f"SELECT COUNT(0) AS rec_cnt FROM {self.database_name}.{self.node_table_name}"
        df = tdml.DataFrame.from_query(SQL).to_pandas()
        rec_cnt = int(df.iloc[0,0])

        return f"Node Table [{self.node_table_name}]: Node ID [{self.node_id_column_name}]. # of Nodes = {rec_cnt}."


    #########################################################
    # Get and set the from node id from label for source id #
    #########################################################
    def set_node_id_from_label_source(self, 
                                      label, 
                                      nodetype=None, 
                                      if_exists = 'replace', 
                                      show_query=False):
        if self.node_table_name is None:
            raise ValueError("Missing Node Table Name (node_table_name)")
        if self.node_id_column_name is None:
            raise ValueError("Missing Node column Name (node_id_column_name)")
        if self.node_label_column_name is None:
            raise ValueError("Missing Node Label Column Name (node_label_column_name)")
        cond1 = ""
        if nodetype is not None and self.node_type_column_name is None:
            raise ValueError("Node Type column name is not defined yet (node_type_column_name)")

        if self.source_table_name is None:
            self.source_table_name = "graph_source_vt"
        if self.source_node_name is None:
            self.source_node_name = "node_id"

        # Create source id table if not exists
        try:
            SQL = f"CREATE MULTISET VOLATILE TABLE {_get_user()}.{self.source_table_name} ({self.source_node_name} BIGINT) UNIQUE PRIMARY INDEX ({self.source_node_name}) ON COMMIT PRESERVE ROWS"
            tdml.execute_sql(SQL)
        except:
            pass

        if if_exists == 'replace':
            tdml.execute_sql(f"DELETE FROM {_get_user()}.{self.source_table_name}")


        if label == 'ALL':
            if nodetype is None:
                SQL = f"""INSERT INTO {_get_user()}.{self.source_table_name}
                          SELECT CAST({self.node_id_column_name} AS BIGINT) AS new_node_id 
                          FROM {self.database_name}.{self.node_table_name}
                          WHERE new_node_id NOT IN
                            (SELECT {self.source_node_name} FROM {_get_user()}.{self.source_table_name})"""
            else:
                SQL = f"""INSERT INTO {_get_user()}.{self.source_table_name}
                          SELECT CAST({self.node_id_column_name} AS BIGINT) AS new_node_id
                          FROM {self.database_name}.{self.node_table_name}
                          WHERE  {self.node_type_column_name} = '{nodetype}'
                          AND new_node_id NOT IN
                           (SELECT {self.source_node_name} FROM {_get_user()}.{self.source_table_name})"""
        else:
            if nodetype is not None:
                cond1 = f" AND {self.node_type_column_name} = '{nodetype}' "
            else:
                cond1 = ""

            SQL = f"""INSERT INTO {_get_user()}.{self.source_table_name}
                      SELECT {self.node_id_column_name} AS new_node_id 
                      FROM {self.database_name}.{self.node_table_name}
                      WHERE {self.node_label_column_name} LIKE '{label}'
                      {cond1}
                      AND new_node_id NOT IN
                       (SELECT {self.source_node_name} FROM {_get_user()}.{self.source_table_name})"""
        if show_query:
            print(SQL)
        tdml.execute_sql(SQL)
        df = tdml.DataFrame(tdml.in_schema(_get_user(), self.source_table_name))
        return df

    #########################################################
    # Get and set the from node id from label for source id #
    #########################################################
    def set_node_id_from_id(self, 
                            ids=None, 
                            if_exists = 'replace', 
                            show_query=False):
        if self.node_table_name is None:
            raise ValueError("Missing Node Table Name (node_table_name)")
        if self.node_id_column_name is None:
            raise ValueError("Missing Node column Name (node_id_column_name)")

        if self.source_table_name is None:
            self.source_table_name = "graph_source_vt"
        if self.source_node_name is None:
            self.source_node_name = "node_id"

        # Create source id table if not exists
        try:
            SQL = f"CREATE MULTISET VOLATILE TABLE {_get_user()}.{self.source_table_name} ({self.source_node_name} BIGINT) UNIQUE PRIMARY INDEX ({self.source_node_name}) ON COMMIT PRESERVE ROWS"
            tdml.execute_sql(SQL)
        except:
            pass

        if if_exists == 'replace':
            tdml.execute_sql(f"DELETE FROM {_get_user()}.{self.source_table_name}")

        if ids is None:
            SQL = f"""INSERT INTO {_get_user()}.{self.source_table_name}
                      SELECT {self.node_id_column_name} AS new_node_id 
                      FROM {self.database_name}.{self.node_table_name}
                      WHERE new_node_id NOT IN
                        (SELECT {self.source_node_name} FROM {_get_user()}.{self.source_table_name})"""
        elif isinstance(ids, list):
            SQL = f"""INSERT INTO {_get_user()}.{self.source_table_name}
                      SELECT {self.node_id_column_name} AS new_node_id 
                      FROM {self.database_name}.{self.node_table_name}
                      WHERE {self.node_id_column_name} IN ({','.join(str(i) for i in ids)})
                      AND new_node_id NOT IN
                        (SELECT {self.source_node_name} FROM {_get_user()}.{self.source_table_name})"""
        else:
            SQL = f"""INSERT INTO {_get_user()}.{self.source_table_name}
                      SELECT {self.node_id_column_name} AS new_node_id 
                      FROM {self.database_name}.{self.node_table_name}
                      WHERE {self.node_id_column_name} = {ids}
                      AND new_node_id NOT IN
                        (SELECT {self.source_node_name} FROM {_get_user()}.{self.source_table_name})"""

        if show_query:
            print(SQL)
        tdml.execute_sql(SQL)
        df = tdml.DataFrame(tdml.in_schema(_get_user(), self.source_table_name))
        return df


    #########################################################
    # Get and set the from node id from label for target id #
    #########################################################
    def set_node_id_from_label_target(self, label, show_query=False):
        if self.node_table_name is None:
            raise ValueError("Missing Node Table Name (node_table_name)")
        if self.node_id_column_name is None:
            raise ValueError("Missing Node column Name (node_id_column_name)")
        if self.node_label_column_name is None:
            raise ValueError("Missing Node Label Column Name (node_label_column_name)")
        SQL = f"""SELECT {self.node_id_column_name} AS node_id 
                  FROM {self.database_name}.{self.node_table_name}
                  WHERE {self.node_label_column_name} Like '%{label}%'"""
        if show_query:
            print(SQL)
        df = tdml.DataFrame.from_query(SQL).to_pandas().reset_index()
        self.target_id = int(df.iloc[0,0])
        return self.target_id


    #########################################
    # Return single node id from node label #
    #########################################
    def get_id_from_label(self, label, show_query = False):
        if self.node_table_name is None:
            raise ValueError("Missing Node Table Name (node_table_name)")
        if self.node_id_column_name is None:
            raise ValueError("Missing Node column Name (node_id_column_name)")
        if self.node_label_column_name is None:
            raise ValueError("Missing Node Label Column Name (node_label_column_name)")
        SQL = f"""SELECT {self.node_id_column_name} AS node_id 
                  FROM {self.database_name}.{self.node_table_name}
                  WHERE {self.node_label_column_name} Like '%{label}%'"""
        if show_query:
            print(SQL)
        df = tdml.DataFrame.from_query(SQL).to_pandas().reset_index()
        id = int(df.iloc[0,0])
        return id



    ########################################################################
    # Identify the topology from the list of a single OR list of source id #
    ########################################################################
    def td_topology(self, 
                    edge_pattern = None,
                    max_path_length = 100,
                    weight_filter = None,
                    return_data = 'P', 
                    output_table = None,
                    temp_output_table = True,
                    show_query = False):

        if self.edge_table_name is None:
            raise ValueError("Missing Edge Table Name (edge_table_name)")
        if self.edge_from_node_column_name is None:
            raise ValueError("Missing Edge FROM Column name (edge_from_node_column_name)")
        if self.edge_to_node_column_name is None:
            raise ValueError("Missing Edge TO Column name (edge_to_node_column_name)")
        if edge_pattern is not None and self.edge_type_column_name is None:
            raise ValueError("Missing Edge Type for edge pattern(edge_type_column_name)")
        if weight_filter is not None and self.edge_weight_column_name is None:
            raise ValueError("Missing weight column (edge_weight_column_name)")
        if self.source_table_name is None or self.source_node_name is None:
            raise ValueError("Missing source IDs. Use either set_node_id_from_id or set_node_id_from_label_source function to add source nodes")

        if self.edge_weight_column_name is None or self.edge_weight_column_name=="" :
            weight_column_adj = "NULL"
        else:
            weight_column_adj = f"'{self.edge_weight_column_name}'"

        if self.datetime_column_name is None or self.datetime_column_name=="" :
            datetime_column_adj = "NULL"
        else:
            datetime_column_adj = f"'{self.datetime_column_name}'"

        if self.edge_weight_column_name is None:
            weight_type_adj = "NULL"
        elif self.weight_type is None:
            weight_type_adj = "NULL"
        elif self.weight_type in ['P', 'p']:
            weight_type_adj = "'P'"
        else:
            weight_type_adj = "'W'"

        if weight_filter is None or weight_filter=="":
            weight_filter_adj = "NULL"
        else:
            weight_filter_adj = f"'{weight_filter}'"

        if return_data.upper() not in ['P','N','C']:
            raise ValueError("return_data must be P(path) or N(node) or C(Closeness Centrality) only!!!")

        if output_table is None or output_table=="" :
            output_table_adj = "NULL"
            temp_output_table_adj = "NULL"
        else:
            self.last_outputtable = output_table
            output_table_adj = f"'{output_table}'"
            if temp_output_table:
                temp_output_table_adj = "1"
                self.last_outputtable_kind = "V"
            else:
                temp_output_table_adj = "0"
                self.last_outputtable_kind = "T"
            self.topology_path_result_table = output_table

        self.max_path_length = max_path_length

        if edge_pattern is None:
          adj_edge_pattern = 'NULL'
        else:
          pattern_str = []
          for pattern1 in edge_pattern:
              if isinstance(pattern1,list):
                  pattern_str.append(','.join(pattern1))
              else:
                  pattern_str.append(pattern1)
          adj_edge_pattern = "'" + "|".join(pattern_str) + "'"

        SQL = f"""CALL {self.graphdb}.graph_topology_sp('{self.database_name}',
                                                        '{self.edge_table_name}',
                                                        '{self.edge_from_node_column_name}',
                                                        '{self.edge_to_node_column_name}', 
                                                         {weight_column_adj}, 
                                                        '{self.edge_type_column_name}',
                                                         {datetime_column_adj},
                                                        '{self.source_table_name}',
                                                        '{self.source_node_name}',
                                                         {weight_type_adj},
                                                         {weight_filter_adj},
                                                         {max_path_length},
                                                         {adj_edge_pattern},
                                                        '{return_data.upper()}',
                                                         {output_table_adj},
                                                         {temp_output_table_adj}
                                                        );"""
        if show_query:
            print(SQL)
        result = tdml.execute_sql(SQL)
        rows0 = result.fetchall()

        if output_table is None:
            result.nextset()
            rows1 = result.fetchall()
            # All returned paths in dataframe, 1 row per path
            if return_data.upper() == 'P':
                df = pd.DataFrame(rows1, columns=["fullpath","weight","path_level"])
                self.topology_path_result_column = "fullpath"
            else:
                df = pd.DataFrame(rows1, columns=["Node","path_level","weight"])
                self.topology_path_result_column = "node"
        else:
            if temp_output_table:
                df = tdml.DataFrame(tdml.in_schema(_get_user(), output_table))
            else:
                df = tdml.DataFrame(output_table)

            if return_data.upper() == 'P':
                self.topology_path_result_column = "fullpath"
            else:
                self.topology_path_result_column = "node"
    
        return(df)


    ######################################################
    # Caculate the shortpath from source_id to target_id #
    ######################################################
    def td_shortest_path(self, 
                         source = None, 
                         target = None, 
                         max_path_length=100, 
                         output_table = None, 
                         temp_output_table = True,
                         show_query = False ):
        if self.edge_table_name is None:
            raise ValueError("Missing Edge Table Name (edge_table_name)")
        if self.edge_from_node_column_name is None:
            raise ValueError("Missing Edge FROM Column name (edge_from_node_column_name)")
        if self.edge_to_node_column_name is None:
            raise ValueError("Missing Edge TO Column name (edge_to_node_column_name)")

        if isinstance(source, int):
            cur_source_id = source
        elif isinstance(source, str):
            cur_source_id = self.get_id_from_label(source, show_query)
        else:
            raise ValueError("source must be integer (node id) or string (node label)")

        if isinstance(target, int):
            cur_target_id = target
        elif isinstance(target, str):
            cur_target_id = self.get_id_from_label(target)
        else:
            raise ValueError("target must be integer (node id) or string (node label)")


        if self.edge_weight_column_name is None or self.edge_weight_column_name=="" :
            weight_column_adj = "NULL"
        else:
            weight_column_adj = f"'{self.edge_weight_column_name}'"


        if output_table is None or output_table=="" :
            output_table_adj = "NULL"
            temp_output_table_adj = "0"
        else:
            output_table_adj = f"'{output_table}'"
            self.shortpath_path_result_table = output_table
            if temp_output_table:
              temp_output_table_adj = "1"
            else:
              temp_output_table_adj = "0"

        self.max_path_length = max_path_length

        if output_table is None or output_table=="" :
            output_table_adj = "NULL"
            temp_output_table_adj = "NULL"
        else:
            self.last_outputtable = output_table
            output_table_adj = f"'{output_table}'"
            if temp_output_table:
                temp_output_table_adj = "1"
                self.last_outputtable_kind = "V"
            else:
                temp_output_table_adj = "0"
                self.last_outputtable_kind = "T"
            self.topology_path_result_table = output_table


        SQL = f"""CALL {self.graphdb}.graph_shortest_path_sp('{self.database_name}',
                                                             '{self.edge_table_name}',
                                                             '{self.edge_from_node_column_name}',
                                                             '{self.edge_to_node_column_name}', 
                                                              {weight_column_adj}, 
                                                              {cur_source_id}, 
                                                              {cur_target_id}, 
                                                              {max_path_length},
                                                              {output_table_adj},
                                                              {temp_output_table_adj}
                                                             );"""
        if show_query:
            print(SQL)
        result = tdml.execute_sql(SQL)
        rows0 = result.fetchall()

        if output_table is None:
            result.nextset()
            rows1 = result.fetchall()
            # All returned paths in dataframe, 1 row per path
            df = pd.DataFrame(rows1, columns=["fullpath","weight"])
            self.topology_path_result_column = "fullpath"
        else:
            if temp_output_table:
                df = tdml.DataFrame(tdml.in_schema(_get_user(), output_table))
            else:
                df = tdml.DataFrame(output_table)
            self.topology_path_result_column = "fullpath"
    
        return(df)


    ################################################################################
    # Converting existing path from node id to node label with optional attributes #
    ################################################################################
    def td_graph_path_decode(self, 
                             input_table = None,
                             input_table_kind = 'V', 
                             edge_attributes = None, 
                             output_table = None, 
                             temp_output_table = True,
                             include_plot = False,
                             show_query = False):
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
            input_table_adj = self.last_outputtable
            input_table_kind_adj = self.last_outputtable_kind
        else:
            input_table_adj = input_table
            input_table_kind_adj = input_table_kind
        if input_table_adj is None:
            raise ValueError("Missing tablename with pathname (input_table)")
       

        if self.topology_path_result_table is None or self.topology_path_result_column is None:
            raise ValueError("Missing topology_path_result_table, please run td_topology function with output table!!!")

        if self.edge_weight_column_name is None or self.edge_weight_column_name=="" :
            weight_column_adj = "NULL"
        else:
            weight_column_adj = f"'{self.edge_weight_column_name}'"

        if edge_attributes is None:
            if self.edge_attributes is None:
                raise ValueError("Missing Edge Attribute column(s) (edge_attributes)!!!")
        else:
            if isinstance(edge_attributes, list):
                self.edge_attributes = edge_attributes
            else:
                self.edge_attributes = [edge_attributes]

        if output_table is None:
            output_table_adj = 'NULL'
            temp_output_table_adj = "0"
        else:
            self.topology_path_decode_table = output_table
            output_table_adj = f'{output_table}'
            self.shortpath_path_result_table = output_table
            if temp_output_table:
              temp_output_table_adj = "1"
            else:
              temp_output_table_adj = "0"

        SQL = f"""CALL {self.graphdb}.graph_path_decode_sp('{self.database_name}',
                                                           '{input_table_adj}',
                                                           '{input_table_kind_adj}',
                                                           '{self.topology_path_result_column}',
                                                           '{self.edge_table_name}',
                                                           '{self.edge_from_node_column_name}',
                                                           '{self.edge_to_node_column_name}',
                                                           {weight_column_adj},
                                                           '{"|".join(self.edge_attributes)}',
                                                           '{self.node_table_name}',
                                                           '{self.node_id_column_name}',
                                                           '{self.node_label_column_name}',
                                                           {output_table_adj},
                                                           {temp_output_table_adj}
                                                           )"""

        if show_query:
            print(SQL)

        result = tdml.execute_sql(SQL)
        rows0 = result.fetchall()
        result.nextset()
        rows1 = result.fetchall()
        local_df = pd.DataFrame(rows1, columns=["path_level","from_id", "to_id","n1_label"] + self.edge_attributes + ["n2_label", "weight"] )
        local_df = local_df.reset_index(drop=True)

        if include_plot:
            path_label_df = local_df
            seen = {}
            path_label_df['new_from_id'] = [seen.setdefault(x, len(seen)) for x in path_label_df["from_id"].to_list()]
            path_label_df['new_to_id'] = [seen.setdefault(x, len(seen)) for x in path_label_df["to_id"].to_list()]
            source = path_label_df['new_from_id'].to_list()
            target = path_label_df['new_to_id'].to_list()
            value = path_label_df['weight'].to_list()
            df_combined = pd.DataFrame({
                "new_id": pd.concat([path_label_df["new_from_id"], path_label_df["new_to_id"]], ignore_index=True),
                "label": pd.concat([path_label_df["n1_label"], path_label_df["n2_label"]], ignore_index=True)
            }).drop_duplicates()
            labels = df_combined.label.to_list()
            fig = go.Figure(data=[go.Sankey(
                node=dict(
                    pad=5,                 # space between nodes
                    thickness=10,           # node thickness
                    label=labels,
                ),
                link=dict(
                    source=source,
                    target=target,
                    value=value,
                    hovertemplate="From %{source.label} to %{target.label}: %{value}<extra></extra>"
                )
            )])
            fig.update_layout(font_size=10, width=900, height=600)
            return(local_df, fig)
        else:
            return(local_df)



    ###############################################################################
    # Decode node information from node id to node label with optional attributes #
    ###############################################################################
    def td_graph_node_decode(self, input_table, node_attributes=None, output_table = None, show_query = False):
        if self.node_table_name is None:
            raise ValueError("Missing Node Table Name (node_table_name)")
        if self.node_id_column_name is None:
            raise ValueError("Missing Node column Name (node_id_column_name)")
        if self.node_label_column_name is None:
            raise ValueError("Missing Node Label Column Name (node_label_column_name)")
        if input_table is None:
            raise ValueError("Missing tablename with pathname (input_table)")

        if self.node_type_column_name is None:
            adj_node_type = ""
        else:
            adj_node_type = f'"{self.node_type_column_name}" AS node_type,'

        if node_attributes is None:
            adj_node_attributes = ""
        elif isinstance(node_attributes,list):
            adj_node_attributes = ',"' + '","'.join(node_attributes) + '"'
        else:
            adj_node_attributes = f', "{node_attributes}"'

        SQL = f"""SELECT 
                   i.path_level,
                   i.weight,
                   {adj_node_type}
                   {self.node_label_column_name} AS node_name
                   {adj_node_attributes}
                FROM {self.database_name}.{input_table} i
                LEFT JOIN {self.node_table_name} n
                ON (i.Node_id = n.{self.node_id_column_name})"""
        if show_query:
            print(SQL)

        df = tdml.DataFrame.from_query(SQL).to_pandas()
        return(df)

    #########################################################################################
    # PageRank algorithm is used to measure the importance of each node based on the number #
    # of incoming relationships and the rank of the related source nodes.                   #
    #########################################################################################
    def td_pagerank(self,
                    directed = True, 
                    damping = 0.85,
                    max_iterations = 100, 
                    tolerance = 1e-8,
                    output_table = None, 
                    temp_output_table = True,
                    show_query = False):

        if self.edge_table_name is None:
            raise ValueError("Missing Edge Table Name (edge_table_name)")
        if self.edge_from_node_column_name is None:
            raise ValueError("Missing Edge FROM Column name (edge_from_node_column_name)")
        if self.edge_to_node_column_name is None:
            raise ValueError("Missing Edge TO Column name (edge_to_node_column_name)")

        if self.edge_weight_column_name is None or self.edge_weight_column_name=="" :
            weight_column_adj = "NULL"
        else:
            weight_column_adj = f"'{self.edge_weight_column_name}'"

        if directed:
            directed_adj = "'Y'"
        else:
            directed_adj = "'N'"


        if output_table is None or output_table=="" :
            output_table_adj = "NULL"
            temp_output_table_adj = "0"
        else:
            self.pr_table_name = output_table
            self.pr_node_name = "node"
            self.pr_score_name = "pr_score"
            output_table_adj = f"'{output_table}'"
            if temp_output_table:
              temp_output_table_adj = "1"
            else:
              temp_output_table_adj = "0"


        SQL = f"""CALL {self.graphdb}.graph_pagerank_sp('{self.database_name}',
                                                        '{self.edge_table_name}',
                                                        '{self.edge_from_node_column_name}',
                                                        '{self.edge_to_node_column_name}', 
                                                         {weight_column_adj},
                                                         {directed_adj},
                                                         {damping},
                                                         {max_iterations},
                                                         {tolerance},
                                                         {output_table_adj},
                                                         {temp_output_table_adj},
                                                         iter
                                                        );"""
        if show_query:
            print(SQL)
        # Execute PageRank SP
        result = tdml.execute_sql(SQL)
        # Get executed iteration
        iters = result.fetchall()[0][0]


        # Get Resultset
        result = tdml.DataFrame.from_query("SELECT node, pr_score FROM pr_sp_result_vt").sort("pr_score", ascending=False)
        return result, iters


    #############################################################################################
    # The Louvain algorithm is a method for detecting communities (clusters) in large networks. #
    # It works with PR score from Page Rank                                                     #
    #############################################################################################
    def td_louvain_community(self,
                             directed = True,
                             p_threshold = 0.5,                             
                             max_iterations = 100,
                             p_resolution = 1.0,
                             output_table = None, 
                             temp_output_table = True,
                             show_query = False):
        # Check edge info
        if self.edge_table_name is None:
            raise ValueError("Missing Edge Table Name (edge_table_name)")
        if self.edge_from_node_column_name is None:
            raise ValueError("Missing Edge FROM Column name (edge_from_node_column_name)")
        if self.edge_to_node_column_name is None:
            raise ValueError("Missing Edge TO Column name (edge_to_node_column_name)")

        # Check PR info
        if self.pr_table_name is None:
            raise ValueError("Missing PageRank Score Table Name (pr_table_name)")
        if self.pr_node_name is None:
            raise ValueError("Missing PageRank Node Name (pr_node_name)")
        if self.pr_score_name is None:
            raise ValueError("Missing PageRank score Table Name (pr_score_name)")

        # Check output table info
        if output_table is None:
            raise ValueError("Please provide output Table Name and or Table Type (output_table & temp_output_table)")

        if self.edge_weight_column_name is None or self.edge_weight_column_name=="" :
            weight_column_adj = "NULL"
        else:
            weight_column_adj = f"'{self.edge_weight_column_name}'"

        if directed:
            directed_adj = "1"
        else:
            directed_adj = "0"

        if temp_output_table:
            temp_output_table_adj = "1"
        else:
            temp_output_table_adj = "0"


        SQL = f"""CALL {self.graphdb}.graph_louvain_communities_sp('{self.database_name}',
                                                                   '{self.edge_table_name}',
                                                                   '{self.edge_from_node_column_name}',
                                                                   '{self.edge_to_node_column_name}', 
                                                                    {weight_column_adj},
                                                                    {directed_adj},
                                                                   '{self.pr_table_name}',
                                                                   '{self.pr_node_name}',
                                                                   '{self.pr_score_name}',
                                                                    {p_threshold},
                                                                    {max_iterations},
                                                                    {p_resolution},
                                                                   '{output_table}',
                                                                    {temp_output_table_adj},
                                                                    p_iterations, p_communities, p_nodes, p_modularity
                                                                    );"""

        if show_query:
            print(SQL)
        # Execute Louvain Communities SP
        result = tdml.execute_sql(SQL).fetchall()
        # Get executed iteration

        p_iterations  = result[0][0]
        p_communities = result[0][1]
        p_nodes       = result[0][2]
        p_modularity  = result[0][3]

        # Get Resultset
        if temp_output_table:
            df = tdml.DataFrame(output_table).sort("comm_id")
        else:
            df = tdml.DataFrame(tdml.in_schema(self.database_name, output_table)).sort("comm_id")

        return df, {"iterations": p_iterations, "communities": p_communities, "node": p_nodes, "modularity": p_modularity}


############################
# End of td_graph_function #
############################
