import pandas as pd
from functools import reduce
from six import string_types
import numbers
from collections import abc

def process_df_for_widget(df_traffic, aggregation_columns=None, 
                          time_column="TIME", value_column="BW", 
                          top_flows_to_show=5):
    '''
    Adjusts a TS data frame to be depicted in a time series widget.
    
    
    The data frame must contain a time column, and a value column. Only one value column is permitted.
    The rest of the columns are characteristics of the time series.
    One can filter the aggregation columns, by default it uses all of them.
    
    Using the aggregation columns, the code calculates the top contributiors of the data frame.
    It then creates and returns two data frames: graph_df and table_df,
    which are suitable to be depicted in a graph and a table in the widget.
    '''
    
    # Although Pandas defaults into non making any inline operation, 
    # let us not risk it and work with a copy of the df.
    df_traffic = df_traffic.copy()

    # Get the list of aggregation columns. It defaults to any non-time, non-value column in the df.
    if aggregation_columns is None:
        aggregation_columns = list(set(df_traffic.columns) - {time_column, value_column})
        aggregation_columns = sorted(aggregation_columns)
    else:
        aggregation_columns = list(aggregation_columns)

    # Find top flows, if columns are aggregated, if not, then we only mantain the total value in time.
    if aggregation_columns:
        top_flows = df_traffic.groupby(by=aggregation_columns).sum().reset_index().sort_values(value_column, ascending=False)
        top_flows = top_flows.head(top_flows_to_show)

        # filter all non top flows, and summarize them as "Others".
        filtered_df = df_traffic.merge(top_flows.rename(columns={value_column:"NULL_CHECK"}), on=tuple(set(aggregation_columns)), how="left")
        filtered_df.loc[filtered_df.NULL_CHECK.isnull(), aggregation_columns] = "Others"
        filtered_df = filtered_df.groupby(list(set(aggregation_columns) | {time_column})).sum()[value_column].reset_index()

        aggregation_column_name = '-'.join([str(column) for column in aggregation_columns])
        aggregation_column = reduce(lambda x,y: x + '-' + y, [filtered_df[column].astype(str) for column in aggregation_columns])

        filtered_df[aggregation_column_name] = aggregation_column
    else:
        filtered_df = df_traffic.groupby(time_column)[value_column].sum().reset_index()
        aggregation_column_name = "TOTAL"
        filtered_df["TOTAL"] = aggregation_column_name
    
    # Create graph and table dfs.
    # The table df does not contain any TIME information, it shows the sum in time over the remaining flows.
    table_df = filtered_df.groupby(list(set(aggregation_columns) | {aggregation_column_name})).mean()[value_column].sort_values(ascending=False)
    graph_df = filtered_df
    
    graph_df = graph_df[[aggregation_column_name, time_column, value_column]]
    graph_df = graph_df.set_index([aggregation_column_name, time_column]).unstack(aggregation_column_name)
    graph_df.columns = graph_df.columns.get_level_values(1)
    graph_df = graph_df[list(table_df.reset_index()[aggregation_column_name])]
    
    # remove the aggregation column in the table if there are more than one selected column
    if len(aggregation_columns) > 1:
        table_df = table_df.reset_index().drop(aggregation_column_name, axis=1)
    else:
        table_df = table_df.reset_index()

    return table_df, graph_df

def hover(hover_color="#F0F0F0"):
    '''
    Hover function, I got them directly from the pandas style documentation page.
    It lets you highlight in a table the row where the mouse is located.
    '''
    return dict(selector="tr:hover",
                    props=[("background-color", "%s" % hover_color)])

def get_traffic_matrix(df_traffic):
    return df_traffic

def get_egress_traffic(df_traffic, asns=None, value_column="BW", egress_link_column="EGRESS_LINK", ingress_link_column="INGRESS_LINK"):
    # for egress, we remove all the ingress link information
    egress_df = df_traffic.drop(ingress_link_column, axis=1)
    if asns is not None and asns:
        egress_df = egress_df[egress_df.DST_AS.isin(asns)]
    return egress_df.groupby(list(set(egress_df.columns) - {value_column}))["BW"].sum().reset_index()

def get_ingress_traffic(df_traffic, asns=None, value_column="BW", egress_link_column="EGRESS_LINK", ingress_link_column="INGRESS_LINK"):
    # for egress, we remove all the ingress link information
    ingress_df =  df_traffic.drop(egress_link_column, axis=1)
    if asns is not None and asns:
        ingress_df = ingress_df[ingress_df.SRC_AS.isin(asns)]
    return ingress_df.groupby(list(set(ingress_df.columns) - {value_column}))["BW"].sum().reset_index()

def apply_changes(df_traffic, changes_dict, column_link, column_prefixes):
    """
    Applies the changes defined in a change dictionary to a data frame.
    Basically any key in the dict is replaced in the column_link with the value.
    """
    
    # I wanted a data frame operation for this function, so I create a df with the
    # dictionary, left-join, and replace the value of column_link for the columns with non null columns.
    if not changes_dict:
        raise Exception("Applying an empty change.")
    new_column_link = "".join(["NEW", column_link])
    changes_df = pd.DataFrame.from_dict(changes_dict, orient='index')
    changes_df = changes_df.reset_index()
    changes_df.columns = [column_prefixes, new_column_link]
    resulting_df = df_traffic.merge(changes_df, on=(column_prefixes), how="left")
    resulting_df.loc[~resulting_df[new_column_link].isnull(), column_link] = resulting_df.loc[~resulting_df[new_column_link].isnull(), new_column_link]
    return resulting_df.drop(new_column_link, axis=1)

def summarize_change_html(changes):
    change_df = pd.DataFrame.from_dict(changes, orient='index')
    change_df.columns = ["New Prefered link"]
    return change_df.style.set_table_attributes('class="table"').render()
   

def load_balancing(df_traffic, interested_links, column_links, column_prefixes, rt, column_time="TIME", column_value="BW"):
    '''
    This load balancing procedure takes care of partial link selection,
    ensuring that no new traffic is attracted to the selected links.
    '''

    # remove the time dimension, to spped up facilitate optimziation
    current_opt = df_traffic.groupby([column_links, column_prefixes])[column_value].sum().reset_index()

    starting_traffic_in_links = current_opt.groupby(column_links)[column_value].sum()[interested_links]
    initial_traffic_in_links = starting_traffic_in_links.sum()

    traffic_per_prefix = current_opt[current_opt[column_links].isin(interested_links)].groupby(column_prefixes)[column_value].sum().sort_values(ascending=False)


    perfect_traffic = starting_traffic_in_links.mean()

    resistance = 0.1
    traffic_increase = 0.05

    max_steps = 20
    changes = {}

    for step in range(0, max_steps):

        # find the top and lower link on our set
        traffic_per_link = current_opt.groupby(column_links)[column_value].sum()[interested_links]
        min_link = traffic_per_link.idxmin()

        # let us find within the local links, the prefixes with more traffic and let us see what happens if
        # we move it to the less link. We do not accept the change if: this increases the overall traffic more than the limit
        # or if the standard deviation increases

        # in networking, it is really complicated to move traffic from one link to other for a specific prefix,
        # it is much easier to move ALL traffic for a prefix over a single link.

        # rank the traffic per prefix in top max
        #top_link_prefixes = current_opt[current_opt[column_links] == max_link].groupby(column_prefixes)[column_value].sum().sort_values(ascending=False)

        # let us try to fit better by moving some traffic to the minimum, let us accept the step if the std is reduced.
        current_std = traffic_per_link.std()


        for prefix, bw in traffic_per_prefix.iteritems():
            # just move traffic of this prefix if there is a valid path there.
            if not min_link in rt[prefix]:
                continue
            attempt_changes = dict(changes)
            attempt_changes[prefix] = min_link
            traffic_after_change = apply_changes(current_opt, attempt_changes, column_links, column_prefixes)

            traffic_per_link_after_change = traffic_after_change.groupby(column_links)[column_value].sum()[interested_links]
            this_std = traffic_per_link_after_change.std()
            this_total_traffic = traffic_per_link_after_change.sum()
            if this_total_traffic - initial_traffic_in_links < -1:
                raise Exception("New traffic in links is less than started, error.")
            if this_total_traffic > initial_traffic_in_links * (1 + traffic_increase):
                #print("Ignoring prefix {}, since it brings {}% more traffic to the links.".format(prefix, this_total_traffic / initial_traffic_in_links  - 1))
                continue
            if this_std < current_std:
                changes[prefix] = min_link
                current_opt = traffic_after_change
                break
    return changes


def simple_load_balancing(df_traffic, column_links, column_prefixes, rt, column_time="TIME", column_value="BW"):
    '''
    This was my first attempt of traffic balancing.
    The df here includes only the links that should be balanced.
    Simple, but it will fail globally since it might attract new traffic to the links.
    '''
   
    # remove the time dimension, to spped up facilitate optimziation
    current_opt = df_traffic.groupby([column_links, column_prefixes])[column_value].sum().reset_index()

    traffic_per_prefix = current_opt.groupby(column_prefixes)[column_value].sum().sort_values(ascending=False)

    perfect_traffic = current_opt.groupby(column_links)[column_value].sum().mean()

    #min_prefix_switch = perfect_traffic / 100
    #traffic_per_prefix = traffic_per_prefix[traffic_per_prefix > min_prefix_switch]


    max_steps = 20
    changes = {}

    for step in range(0, max_steps):
        # find the top and lower link
        traffic_per_link = current_opt.groupby(column_links)[column_value].sum()

        min_link = traffic_per_link.idxmin()

        # in networking, it is really complicated to move traffic from one link to other for a specific prefix,
        # it is much easier to move ALL traffic for a prefix over a single link.

        # rank the traffic per prefix in top max
        #top_link_prefixes = current_opt[current_opt[column_links] == max_link].groupby(column_prefixes)[column_value].sum().sort_values(ascending=False)

        # let us try to fit better by moving some traffic to the minimum, let us accept the step if the std is reduced.
        current_std = traffic_per_link.std()

        for prefix, bw in traffic_per_prefix.iteritems():
            # just move traffic of this prefix if there is a valid path there.
            if not min_link in rt[prefix]:
                continue
            calculating_changes = current_opt.copy()
            calculating_changes.loc[calculating_changes[column_prefixes] == prefix, column_links] = min_link
            this_std = calculating_changes.groupby(column_links)[column_value].sum().std()
            if this_std < current_std:
                changes[prefix] = min_link
                current_opt = calculating_changes
                print(prefix, min_link, this_std)
                break
        
def get_total_per_link(df_traffic, interested_links, column_links, column_time="TIME", column_value="BW"):
    return df_traffic.pipe(lambda x: x[x[column_links].isin(interested_links)]).groupby([column_links, column_time])[column_value].sum().unstack(column_links)
    
# auxiliary functiosn for the json browser.
def extract_values_from_json_object(data):
    '''
    Checks the type of data and returns a set of list of keys (if any) or the string value.
    Only supports, dict and lists or collectors. 
    '''
    if not isinstance(data, dict):
        return None

    values = {}
    for key, potential_value in data.items():
        if isinstance(potential_value, (string_types, numbers.Real)):
            values[key] = potential_value  

    return values


def process_data(data):
    '''
    Checks the type of data and returns a set of list of keys (if any) or the string value.
    Only supports, dict and lists or collectors. 
    '''
    if data is None:
        return (None, None, None)
    elif isinstance(data, dict):
        keys = list(data.keys())
        values = extract_values_from_json_object(data)
        return (list(keys), data, values)
    elif isinstance(data, string_types):
        return (None, str(data), None)
    elif isinstance(data, abc.Iterable):
        processed_data = {}
        for value_n, value in enumerate(data):
            key = "{}: '{}'".format(value_n, str(value)[0:40])
            processed_data[key] = value
        return (list(processed_data.keys()), processed_data, None)

    return (None, str(data), None)
