import pandas as pd
import numpy as np
from onboarding.adapters.gql_api import GQLAPI
from onboarding.utils import get_users_account_ids
import os
import httpx


def main():
    initial_setup = establish_connections()
    crete_hierarchy_levels(initial_setup, CREATE_HIERARCHY_LEVELS_MUTATION)
    # Retrieve existing hierarchy groups
    current_hierarchy_groups_df = fetch_data_from_graphql_api(
        initial_setup,
        "currentHierarchyGroups",
        QUERY_HIERARCHY_GROUPS,
        "hierarchyGroups"
    )
    # If we have existing hierarchy groups, delete them
    if current_hierarchy_groups_df.shape[0] > 0:
        old_hierarchy_groups_ids = current_hierarchy_groups_df.node_id.tolist()
        delete_existing_hierarchy_groups(initial_setup, old_hierarchy_groups_ids, HARD_DELETE_MUTATION)

    # Process the new alignments file
    all_hierarchies_df, top_hierarchies_df, new_alignments_df = create_new_alignments_from_file(initial_setup)
    # Then load the new hierarchy groups
    hierarchies_loaded_list = create_new_hierarchy_groups(
        top_hierarchies_df,
        initial_setup,
        LOAD_NEW_HIERARCHY_GROUPS_MUTATION
    )
    just_loaded_hierarchies_df = fetch_data_from_graphql_api(
        initial_setup,
        "currentHierarchyGroups",
        QUERY_HIERARCHY_GROUPS,
        "hierarchyGroups"
    )
    new_hierarchy_groups = just_loaded_hierarchies_df.node_id.tolist()
    if len(new_hierarchy_groups) == len(hierarchies_loaded_list):
        print("All good, all the hierarchy groups from the file have been loaded successfully")
    else:
        raise Exception("The hierarchy groups in the file doesn't match the just-loaded hierarchy groups in the API")

    # Now let's retrieve existing locations
    old_locations_df = fetch_data_from_graphql_api(
        initial_setup,
        "queryLocations",
        QUERY_LOCATIONS,
        "locations"
    )
    if old_locations_df.shape[0] == 0:
        raise Exception("There are not locations to assign to the hierarchy groups")

    # Let's assign the *existing locations* to the new hierarchy groups
    final_assignments_dict = map_locations_to_hierarchies(
        old_locations_df,
        all_hierarchies_df,
        just_loaded_hierarchies_df,
        initial_setup,
        ADD_LOCATIONS_MUTATION,
        new_alignments_df,
        fetch_data_from_graphql_api
    )
    # Establish the action in the API
    assign_locations_to_hierarchy_groups(final_assignments_dict, initial_setup, ASSIGN_LOCATIONS_MUTATION)
    # Let's query the users existing in the API
    users_df = fetch_data_from_graphql_api(
        initial_setup=initial_setup, operation_name="users_query", operation=QUERY_ALL_USERS, type="users"
    )

    # Assign permissions to users
    users_already_assigned_list = assign_users_permissions(
        new_alignments_df=new_alignments_df,
        current_api_users_df=users_df,
        just_loaded_hierarchies_df=just_loaded_hierarchies_df,
        permission_mutation=HIERARCHY_GROUP_PERMISSION_MUTATION,
        initial_setup=initial_setup,
        post_user_permissions_on_api=post_user_permissions_on_api
    )

    # Assign permissions to corporate users
    assign_corporate_users_permissions(
        just_loaded_hierarchies_df=just_loaded_hierarchies_df,
        current_api_users_df=users_df,
        initial_setup=initial_setup,
        permission_mutation=HIERARCHY_GROUP_PERMISSION_MUTATION,
        users_already_assigned=users_already_assigned_list,
        post_user_permissions_on_api=post_user_permissions_on_api
    )


# Functions section
def establish_connections():
    """
     This function establishes the connection to the graphql API, hello world
    """
    api = input("\nAPI URL here (without 'https://' and quotes):\n")
    token = input("\nAPI token here. Ensure is fresh (without quotes):\n")
    current_account = input("\nAccount to work with (without quotes):\n")
    folder_path = input("\nPath to the new alignments folder:\n")
    ins_folder = os.path.join(folder_path, "Ins (new files for hierarchies)")
    excel_file_path = None
    corporate_users_path = None

    # Loop through each file in the folder once, identifying the Excel and CSV files
    for file in os.listdir(ins_folder):
        if file.endswith(".xlsx") and not excel_file_path:
            excel_file_path = os.path.join(ins_folder, file)
        elif file.endswith(".csv") and not corporate_users_path:
            corporate_users_path = os.path.join(ins_folder, file)

    print(excel_file_path)
    print(corporate_users_path)

    try:
        account_ids = get_users_account_ids(service_url=api, access_token=token)
        gql_api = GQLAPI(service_url=api, access_token=token)

    except Exception as e:
        print("The connection to the API is broken. Please try again.")
        print(str(e))
        return None

    initial_setup = {
        "current_account": current_account,
        "account_ids": account_ids,
        "gql_api": gql_api,
        "excel_file_path": excel_file_path,
        "corporate_users_path": corporate_users_path,
        "folder_path": folder_path
    }
    print("\nConnection established")
    print("Working ... ")
    return initial_setup


def crete_hierarchy_levels(initial_setup, operation):
    """
    This function creates hierarchy levels in the API, in this case it creates
    Region as top level and District as a child
    """
    print("\nCreating hierarchy levels...\n")
    gql_api = initial_setup["gql_api"]

    variables = [
        {"input": {
            "levels": [
                {
                    "name": "Region",
                    "isTopLevel": True
                }
            ]
        }
        },
        {
            "input": {
                "levels": [
                    {
                        "name": "District",
                        "isTopLevel": False,
                        "parentLevelName": "Region"
                    }
                ]
            }
        }
    ]

    for hierarchy_level in variables:
        gql_api.post(
            query=operation,
            operation_name="hierarchyAddLevels",
            variables=hierarchy_level,
            current_account_id=initial_setup["current_account"],
            account_ids=initial_setup["account_ids"]
        )


def fetch_data_from_graphql_api(initial_setup, operation_name, operation, type, extra_variables=None):
    """
    This function fetches the data in the format edges>nodes
    Is used to fetch data like users, hierarchy groups and locations
    it returns a pandas dataframe with all the pages found in the API
    """
    print(f"\nRetrieving {type} from the API...\n")
    if extra_variables:
        variables = extra_variables
    else:
        variables = {}

    gql_api = initial_setup["gql_api"]
    has_more_pages = True
    calls = []

    while has_more_pages:
        response_data = gql_api.post(
            query=operation,
            operation_name=operation_name,
            variables=variables,
            current_account_id=initial_setup["current_account"],
            account_ids=initial_setup["account_ids"]
        )

        calls.append(response_data)
        page_info = response_data["data"][type]["pageInfo"]
        has_more_pages = page_info["hasNextPage"]

        if has_more_pages:
            variables = {"after": page_info["endCursor"]}
            if extra_variables:
                variables = {**variables, **extra_variables}

    nodes_list = []
    for call in calls:
        nodes = call["data"][type]["edges"]
        if isinstance(nodes, list):
            nodes_list = nodes_list + nodes

    df = pd.json_normalize(nodes_list, sep="_")
    print(f"\nSuccessfully fetched {df.shape[0]} nodes of {type}\n")

    return df


def delete_existing_hierarchy_groups(initial_setup, current_hierarchy_groups_list, mutation_operation):
    print("\nDeleting current hierarchy groups...\n")
    gql_api = initial_setup["gql_api"]

    try:
        gql_api.post(
            query=mutation_operation,
            operation_name="droppingExistingHierarchyGroups",
            variables={
                "input": {
                    "account": initial_setup["current_account"],
                    "hierarchyGroups": current_hierarchy_groups_list
                }
            },
            current_account_id=initial_setup["current_account"],
            account_ids=initial_setup["account_ids"]
        )
    except httpx.ReadTimeout:
        print("Timeout error occurred, but continuing since the operation might have completed.")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

    print("\nProceeding despite the ReadTimeout error\n")


def create_new_alignments_from_file(initial_setup):
    """
    This function is used to process the data in the new alignments file,
    returning three dataframes with different format used in next functions
    """
    print("\nProcessing the excel file...\n")
    new_alignments_df = pd.read_excel(initial_setup["excel_file_path"], sheet_name="Sorted by Store #")
    # new_alignments_df.loc[new_alignments_df["District"] == "PMG /District 1", "District"] = "PMG / District 1"
    new_alignments_df.dropna(subset=["District", "REGION / SUPERVISOR", "Location", "Unit"], inplace=True)

    all_hierarchies_df = (
        new_alignments_df
        [["REGION / SUPERVISOR", "District", "Location", "Unit"]]
        .assign(Unit=lambda df: df['Unit'].astype(int))
        .assign(Unit=lambda df: df['Unit'].astype(str))
        .reset_index(drop=True)
    )

    top_hierarchies_df = (
        new_alignments_df
        [["REGION / SUPERVISOR", "District"]]
        .reset_index(drop=True)
    )
    print("\nDONE\n")
    return all_hierarchies_df, top_hierarchies_df, new_alignments_df


def create_new_hierarchy_groups(top_levels_df, initial_setup, create_hierarchies_mutation):
    """
    Creates new hierarchy groups in the API.
    It needs a dataframe of the hierarchy groups in a specific format
    """
    print("\nCreating hierarchy groups in the API...\n")
    gql_api = initial_setup["gql_api"]
    groups_list = []
    first_top_hierarchy_group = set()
    unique_pair_hierarchy_groups = set()

    for i, row in top_levels_df.iterrows():

        region = row["REGION / SUPERVISOR"]
        district_tuple = (region, row["District"])

        region_dict = {
            "name": region,
            "levelName": "Region"
        }
        district_dict = {
            "name": row["District"],
            "levelName": "District",
            "parentGroupName": region
        }

        if region not in first_top_hierarchy_group:
            groups_list.append(region_dict)
            first_top_hierarchy_group.add(region)

        if district_tuple not in unique_pair_hierarchy_groups:
            groups_list.append(district_dict)
            unique_pair_hierarchy_groups.add(district_tuple)

    gql_api.post(
        query=create_hierarchies_mutation,
        operation_name="hierarchyAddGroups",
        variables={
            "input": {
                "groups": groups_list
            }
        },
        current_account_id=initial_setup["current_account"],
        account_ids=initial_setup["account_ids"]
    )
    print("\nDONE\n")
    return groups_list


def create_and_load_locations(new_alignments_df, initial_setup, add_locations_mutation):
    """
    This function will create and load new locations based on the file.
    It is a help function that were used mainly in test scripts
    """
    new_alignments_df["Zip Code"] = new_alignments_df["Zip Code"].astype(str)
    mask = new_alignments_df["Zip Code"].str.contains("-", na=False)
    new_alignments_df.loc[mask, "Zip Code"] = new_alignments_df[mask]["Zip Code"].str.split("-", expand=True)[0]
    new_alignments_df["Zip Code"] = pd.to_numeric(new_alignments_df["Zip Code"], errors="coerce")
    new_alignments_df["Unit"] = new_alignments_df["Unit"].astype(int)

    locations_df = (
        new_alignments_df[["Location", "Address", "City", "State", "Zip Code", "Unit"]]
        .rename(
            columns={
                "Location": "name",
                "Address": "streetAddress",
                "City": "locality",
                "State": "province",
                "Zip Code": "postalCode",
                "Unit": "remoteId"
            }
        )
        .dropna(subset=["name"])
        .replace(np.nan, "", regex=True)
        .apply(lambda col: col.replace(np.nan, ""), axis=1)
        .assign(postalCode=lambda x: x.postalCode.astype(str).str.split(".").str[0])
        .assign(remoteId=lambda x: x.remoteId.astype(str))
    )
    locations_to_load_dict = locations_df.to_dict(orient="records")

    gql_api = initial_setup["gql_api"]
    for obj in locations_to_load_dict:
        gql_api.post(
            query=add_locations_mutation,
            operation_name="locationAdd",
            variables={
                "input": obj
            },
            current_account_id=initial_setup["current_account"],
            account_ids=initial_setup["account_ids"],
        )
    print("The locations have been loaded successfully")
    return locations_to_load_dict


def map_locations_to_hierarchies(
        old_locations_df,
        all_hierarchies_df,
        just_loaded_hierarchies_df,
        initial_setup,
        add_locations_mutation,
        new_alignments_df,
        fetch_data_from_gql_api
):
    """
    This function is used to map locations to new hierarchies.
    It maps the locations from the API with the new hierarchies just loaded.
    The match is established between the Unit column in the file, the remoteId
    of the locations in the API.
    It needs a dataframe containing all the hierarchy tree 'all_hierarchies_df' and
    the 'just_loaded_hierarchies_df' that is the dataframe of the just loaded hierarchies.
    It returns a list of dictionaries containing the hierarchy group-location assignment
    """
    print("\nMapping existing locations with hierarchy groups...\n")
    if (old_locations_df["node_remoteId"] == "").all():
        merged_locations_df = (
            all_hierarchies_df  # File df
            .merge(
                old_locations_df,  # old locations
                how="outer",
                left_on="Location",
                right_on="node_name",
                indicator=True
            )
            .drop(columns=["cursor", "REGION / SUPERVISOR", "node_name"])
            .rename(columns={"node_id": "global_id_locations"})
        )
    else:
        merged_locations_df = (
            all_hierarchies_df  # locations from the new file
            .merge(
                old_locations_df,  # current locations in the API
                how="outer",
                left_on="Unit",
                right_on="node_remoteId",
                indicator=True
            )
            .drop(columns=["cursor", "REGION / SUPERVISOR", "node_name"])
            .rename(columns={"node_id": "global_id_locations"})
        )
    # The other sub-datasets marked with 'left_only' or 'right_only' indicates the following:
    # 'left_only' signifies that these are new records that were NOT found in the current locations from the API.
    # 'right_only' signifies that these records are only found in the dataframe of locations that already exist in
    # the API ('old locations'). Since no matching record was found for this location, it is advisable to delete or
    # deactivate it, as it is no longer present in the new alignments sheet.

    new_locations_from_file_df = merged_locations_df[merged_locations_df["_merge"] == "left_only"]
    locations_api_not_excel = merged_locations_df[merged_locations_df["_merge"] == "right_only"]

    locations_df = merged_locations_df[merged_locations_df["_merge"] == "both"].drop(columns=["_merge"])
    base_path = os.path.dirname(os.path.dirname(initial_setup["corporate_users_path"]))
    outs_path = os.path.join(base_path, "Outs (results of the script)")

    if new_locations_from_file_df.shape[0] > 0:
        new_locations_from_file_df.to_csv(
            outs_path + "/new_locations_found_in_the_file_and_not_in_API.csv", index=False
        )
        print(
            f"Locations that are only found in the file but not in the API:{locations_api_not_excel.shape[0]},\
             this would mean we need to add the new locations to the API..."
        )

        locations = new_locations_from_file_df.Location.unique()
        locations_df = (
            new_alignments_df[["Location", "Address", "City", "State", "Zip Code", "Unit"]]
            [new_alignments_df.Location.isin(locations)]
            .rename(
                columns={
                    "Location": "name",
                    "Address": "streetAddress",
                    "City": "locality",
                    "State": "province",
                    "Zip Code": "postalCode",
                    "Unit": "remoteId"
                }
            )
            .dropna(subset=["name"])
            .replace(np.nan, "", regex=True)
            .apply(lambda col: col.replace(np.nan, ""), axis=1)
            .assign(postalCode=lambda x: x.postalCode.astype(str).str.split(".").str[0])
            .assign(remoteId=lambda x: x.remoteId.astype(str))
        )
        locations_to_load_dict = locations_df.to_dict(orient="records")

        gql_api = initial_setup["gql_api"]
        for obj in locations_to_load_dict:
            gql_api.post(
                query=add_locations_mutation,
                operation_name="locationAdd",
                variables={
                    "input": obj
                },
                current_account_id=initial_setup["current_account"],
                account_ids=initial_setup["account_ids"],
            )
        print("The locations have been loaded successfully")

    print("\nFetching updated locations from API...\n")
    current_locations_updated_df = fetch_data_from_gql_api(
        initial_setup,
        "queryLocations",
        QUERY_LOCATIONS,
        "locations"
    )

    if (current_locations_updated_df["node_remoteId"] == "").all():
        merged_locations_df = (
            all_hierarchies_df
            .merge(
                current_locations_updated_df,
                how="outer",
                left_on="Location",
                right_on="node_name",
                indicator=True
            )
            .drop(columns=["cursor", "REGION / SUPERVISOR", "node_name"])
            .rename(columns={"node_id": "global_id_locations"})
        )
    else:
        merged_locations_df = (
            all_hierarchies_df
            .merge(
                current_locations_updated_df,
                how="outer",
                left_on="Unit",
                right_on="node_remoteId",
                indicator=True
            )
            .drop(columns=["cursor", "REGION / SUPERVISOR", "node_name"])
            .rename(columns={"node_id": "global_id_locations"})
        )

    new_locations_from_file_df = merged_locations_df[merged_locations_df["_merge"] == "left_only"]
    locations_api_not_excel = merged_locations_df[merged_locations_df["_merge"] == "right_only"]

    locations_df = merged_locations_df[merged_locations_df["_merge"] == "both"].drop(columns=["_merge"])
    base_path = os.path.dirname(os.path.dirname(initial_setup["corporate_users_path"]))
    outs_path = os.path.join(base_path, "Outs (results of the script)")

    if new_locations_from_file_df.shape[0] > 0:
        print("WARNING We still have locations that haven't been loaded")
        print(new_locations_from_file_df)

    if locations_api_not_excel.shape[0] > 0:
        locations_api_not_excel.to_csv(outs_path + "/api_locations_not_found_in_the_file.csv", index=False)
        print(
            f"Locations that are only found in the API but not in the file:{new_locations_from_file_df.shape[0]},\
            this would suggest deprecated records"
        )

    districts_df = (
        just_loaded_hierarchies_df[just_loaded_hierarchies_df["node_isTop"] == False]
        .rename(columns={"node_name": "current_district", "node_id": "global_id_district"})
        [["current_district", "global_id_district"]]
    )
    final_assignments_df_merged = (
        locations_df  # We keep just the old locations here, the coincident locations in both sources
        .merge(
            districts_df,  # we merge the new districts right here
            how="outer",
            left_on="District",
            right_on="current_district",
            indicator=True
        )
    )
    # right only indicates that in the join between locations and districts, (the join is being made by 'district name')
    # there are records that only you can find in the new district alignments sheet. This is not possible because the
    # table of locations previously was merged using the locations ID 'Unit' with the new locations in the file
    # (and that's why we have the district column in the locations_df).
    # So, right only existing records are impossible to have
    # Left only records mean there are districts that we have in the API that the new file does not.
    # This would indicate we have outdated records for districts

    mapped_locations_with_districts = final_assignments_df_merged[final_assignments_df_merged["_merge"] == "both"]
    outdated_districts_df = final_assignments_df_merged[final_assignments_df_merged["_merge"] == "left_only"]
    print(f"Mapped records (locations and districts): {mapped_locations_with_districts.shape[0]}")

    base_path = os.path.dirname(os.path.dirname(initial_setup["corporate_users_path"]))
    outs_path = os.path.join(base_path, "Outs (results of the script)")

    if outdated_districts_df.shape[0] > 0:
        outdated_districts_df.to_csv(outs_path + "districts_not_found_in_file_present_on_API.csv", index=False)
        print(
            f"Districts that we have in the API that the new file does not:{outdated_districts_df.shape[0]},\
         this would suggest deprecated records"
        )

    final_assignments_dict = (
        mapped_locations_with_districts
        .groupby("global_id_district", as_index=False)["global_id_locations"]
        .unique()
        .rename(columns={"global_id_district": "group", "global_id_locations": "locations"})
        .to_dict(orient="records")
    )

    for obj in final_assignments_dict:
        if isinstance(obj["locations"], np.ndarray):
            obj["locations"] = obj["locations"].tolist()
    return final_assignments_dict


def assign_locations_to_hierarchy_groups(final_assignments_dict, initial_setup, locations_mutation):
    """
    This function establish the operation of assigning hierarchies with locations in the API
    """
    print("\nAssigning locations to hierarchy groups...\n")

    for obj in final_assignments_dict:
        mutation_variables = {
            "input": {
                "group": obj["group"],
                "locations": obj["locations"],
            }
        }

        gql_api = initial_setup["gql_api"]
        gql_api.post(
            query=locations_mutation,
            operation_name="hierarchyAssignLocations",
            variables=mutation_variables,
            current_account_id=initial_setup["current_account"],
            account_ids=initial_setup["account_ids"]
        )
    print("\nDONE\n")


def prepare_add_users_to_account(new_alignments_df, fake_users=True):
    """
    This function prepares users to be loaded in the API.
    Is mainly used to test scripts that doesn't have users in the account.
    It returns a list of dictionaries containing the variables to be used with
    the addNewUser mutation, and a dataframe containing selected columns to be
    used in the user permissions
    """

    cols_to_keep = ["DM", "DM Email", "REGION / SUPERVISOR", "SUPERVISOR Email", "Location", "District", "Unit"]
    users_df_without_id = new_alignments_df[cols_to_keep]
    users_df_without_id.loc[:, "Unit"] = users_df_without_id.loc[:, "Unit"].copy().astype(int)
    users_df_without_id.loc[:, "Unit"] = users_df_without_id.loc[:, "Unit"].copy().astype(str)

    def get_name_and_lastname(row):
        if row is not None:
            if "/" in row:
                return row.split("/")[-1]
            else:
                return " ".join(row.split(" ")[1:])
        return None

    users_df_without_id.loc[:, "name_lastname_region_supervisor"] = \
        users_df_without_id.loc[:, "REGION / SUPERVISOR"].copy().apply(get_name_and_lastname)

    if fake_users:
        test_users = users_df_without_id["DM"].unique().tolist() + users_df_without_id[
            "name_lastname_region_supervisor"].unique().tolist()
        dict_users_and_email = {}
        for i, name in enumerate(test_users):
            fake_email = f"daniel.lugo+{i * 100}@replypro.com"
            dict_users_and_email[name] = fake_email

        users_df_without_id.loc[:, "new_email_fake"] = \
            users_df_without_id.loc[:, "DM"].copy().replace(dict_users_and_email)
        users_df_without_id.loc[:, "new_email_fake"] = \
            users_df_without_id.loc[:, "name_lastname_region_supervisor"].copy().replace(dict_users_and_email)

    else:
        dm_emails = pd.Series(users_df_without_id['DM Email'].values, index=users_df_without_id['DM'])
        dm_emails = dm_emails.str.strip().str.lower().to_dict()

        name_lastname_region_emails = pd.Series(users_df_without_id['SUPERVISOR Email'].values,
                                                index=users_df_without_id['name_lastname_region_supervisor'])
        name_lastname_region_emails = name_lastname_region_emails.str.strip().str.lower().to_dict()
        dict_users_and_email = {**dm_emails, **name_lastname_region_emails}

    user_add_new_input = []
    for name, email in dict_users_and_email.items():
        name = name.strip()
        list_of_names = name.split(" ")
        first_name = list_of_names[0]
        if len(list_of_names) > 1:
            last_name = list_of_names[1]
        else:
            last_name = ""
        nested_dict = {
            "email": email,
            "firstName": first_name,
            "lastName": last_name,
            "type": "member"
        }
        user_add_new_input.append(nested_dict)

    return user_add_new_input, users_df_without_id


def load_new_users(user_add_new_input, initial_setup, add_user_mutation):
    """
    This function load the users created with the func 'prepare_add_users_to_account'
    :param user_add_new_input: list of dictionaries for the userAddNewToAccount mutation
    """

    qgl_api = initial_setup["gql_api"]
    current_account = initial_setup["current_account"]
    account_ids = initial_setup["account_ids"]

    for user_variable in user_add_new_input:
        qgl_api.post(
            query=add_user_mutation,
            operation_name="myMutationToCreateUsers",
            variables={"input": user_variable},
            current_account_id=current_account,
            account_ids=account_ids
        )
    print("The users were added to the account")


def post_user_permissions_on_api(
        initial_setup,
        permission_mutation,
        users_and_hierarchies_list,
        type_of_user
):

    """
    This function receives a list of dictionaries of users and their hierarchies in global IDs
    to perform the post operation in the API to give users permissions
    :param initial_setup: The dictionary containing all the connection variables
    :param permission_mutation: The mutation operation for users permissions
    :param users_and_hierarchies_list: The list containing users and hierarchies
    :param type_of_user: The type of user: corporate user or normal user
    """
    # Post our mapped users with hierarchies in the API and save the users results
    users_success = []
    for obj in users_and_hierarchies_list:
        post = initial_setup["gql_api"].post(
            query=permission_mutation,
            operation_name="hierarchyGroupPermissionAdd",
            variables={"input": obj},
            current_account_id=initial_setup["current_account"],
            account_ids=initial_setup["account_ids"]
        )
        if post["data"]["hierarchyGroupPermissionAdd"]["success"] is False:
            obj["success"] = False
            users_success.append(obj)
        else:
            obj["success"] = True
            users_success.append(obj)

    users_success_df = pd.DataFrame(users_success)

    base_path = os.path.dirname(os.path.dirname(initial_setup["corporate_users_path"]))
    outs_path = os.path.join(base_path, "Outs (results of the script)")
    users_success_df.to_csv(outs_path + f"/success_in_{type_of_user}_permissions.csv", index=False)
    if len(users_success_df[users_success_df.success == False]) > 0:
        print(
            f"We have {users_success_df[users_success_df.success == False].shape[0]} \
                     records that weren't correctly assigned"
        )
    print("\nDONE\n")


def assign_users_permissions(
        new_alignments_df,
        current_api_users_df,
        just_loaded_hierarchies_df,
        permission_mutation,
        initial_setup,
        post_user_permissions_on_api
):
    """
    Assign users permissions to the new hierarchy groups.
    :param new_alignments_df: The dataframe containing all the Excel file, obtained by the new_alignments func.
    :param current_api_users_df: The users that already exist in the API
    :param just_loaded_hierarchies_df: The hierarchies dataframe queried from the API
    :param permission_mutation: The mutation operation
    :param initial_setup: The dictionary containing the connection variables
    :param post_user_permissions_on_api: The function to do the operation in the API
    """

    print("\nAssigning user permissions ...\n")
    cols_to_keep = ["REGION / SUPERVISOR", "SUPERVISOR Email", "District", "DM Email", "Franchise or Equity"]
    users_and_hierarchies_df = new_alignments_df[cols_to_keep].copy()
    users_and_hierarchies_df["DM Email"] = users_and_hierarchies_df["DM Email"].copy().str.lower().str.strip()

    # Create a specific condition for these users and their hierarchies:
    condition_just_for_eric_and_neptune = (
        users_and_hierarchies_df['REGION / SUPERVISOR']
        .copy()
        .isin(["Sodexo / Elizabeth Nepute", "Provider / Eric Da Costa"])
    )
    condition_just_for_eric_and_neptune2 = users_and_hierarchies_df["SUPERVISOR Email"].copy().isna()

    # Fill the missing emails from those two users
    users_and_hierarchies_df.loc[
        condition_just_for_eric_and_neptune &
        condition_just_for_eric_and_neptune2,
        "SUPERVISOR Email"
    ] = users_and_hierarchies_df["DM Email"].copy()

    # Create two dataframes depending on the above one to stack their columns
    # Region df
    df_region = users_and_hierarchies_df[["REGION / SUPERVISOR", "SUPERVISOR Email", "Franchise or Equity"]].copy()
    df_region = df_region.rename(
        columns={
            "REGION / SUPERVISOR": "Hierarchy name",
            "SUPERVISOR Email": "Email"
        }
    ).drop_duplicates()
    df_region["type hierarchy"] = "Region"

    # District df
    df_district = users_and_hierarchies_df[["District", "DM Email", "Franchise or Equity"]].copy()
    df_district = (
        df_district
        .rename(
            columns={
                "District": "Hierarchy name",
                "DM Email": "Email"
            }
        )
        .drop_duplicates()
    )
    df_district["type hierarchy"] = "District"

    # Concatenate the two hierarchies dataframes
    users_hierarchies_df = pd.concat([df_region, df_district], ignore_index=True)
    users_hierarchies_df = users_hierarchies_df.drop_duplicates()
    users_hierarchies_df["Email"] = users_hierarchies_df["Email"].copy().str.strip().str.lower()

    # Match the users from the API with the ones from the Excel file
    # The left dataframe represents the data from the Excel file, while the right one represents the data from API
    matched_users_df = users_hierarchies_df.merge(
        current_api_users_df,
        how="outer",
        left_on="Email",
        right_on="node_email",
        indicator=True
    )

    base_path = os.path.dirname(os.path.dirname(initial_setup["corporate_users_path"]))
    outs_path = os.path.join(base_path, "Outs (results of the script)")
    # The next dataframe contains only users that were only found in the Excel file, these users don't match the
    # users in the API
    users_file_df = matched_users_df[matched_users_df["_merge"] == "left_only"]
    print(
        f"\nWe have found {len(users_file_df)} users that doesn't have a match with the API users. These users aren't "
        f"considered for anything"
    )
    if len(users_file_df) > 0:
        users_file_df.to_csv(outs_path + "/new_users_found_in_file_that_does_not_have_match_on_API.csv", index=False)
        raise Exception(
            "Users found in the file that do not have a match in the API.\
            Terminating script. Please load those users first."
        )
    # The next dataframe contains users that only were found in the API, none of these users match with the users
    # from the file
    users_api_df = matched_users_df[matched_users_df["_merge"] == "right_only"]
    print(
        f"We have found {len(users_api_df)} users that doesn't have a match with the file users. These users aren't "
        f"considered for anything, this would suggest deprecated users"
    )
    if len(users_api_df) > 0:
        users_api_df.to_csv(outs_path + "/users_on_API_with_no_match_on_file.csv", index=False)

    # We are going to establish permissions to hierarchy groups to those users that we have in the API and in the file
    final_users_df = matched_users_df[matched_users_df["_merge"] == "both"]
    final_users_df = final_users_df.drop(columns=["node_email", "_merge"])
    final_users_df = final_users_df.rename(columns={"node_id": "user_globalID"})

    # Merge the users already matched (the users that are in API and file) with their hierarchies
    # The join is made by the hierarchy name, to map the hierarchy global ID
    mapped_users_hierarchies_df = final_users_df.merge(
        just_loaded_hierarchies_df.rename(
            columns={"node_id": "hierarchy_globalID",
                     "node_name": "hierarchy_name_from_API"
                     }
        )[["hierarchy_globalID", "hierarchy_name_from_API"]],
        how="outer",
        left_on="Hierarchy name",
        right_on="hierarchy_name_from_API",
        indicator=True
    )

    # The right records mean hierarchies found only
    # in the API, this wouldn't have sense if we weren't discarding users who don't exist

    mapped_users_hierarchies_only_on_api_df = mapped_users_hierarchies_df[
        mapped_users_hierarchies_df["_merge"] == "right_only"
        ]

    if mapped_users_hierarchies_only_on_api_df.shape[0] > 0:
        mapped_users_hierarchies_only_on_api_df.to_csv(
            outs_path + "/hierarchies_on_API_that_doesnt_have_user_assigned.csv", index=False
        )
        print(f"We have {len(mapped_users_hierarchies_only_on_api_df)} hierarchies that weren't assigned to users.\
            Likely because the users doesn't exist in the API"
              )

    mapped_users_hierarchies_final_df = mapped_users_hierarchies_df[mapped_users_hierarchies_df["_merge"] == "both"]
    mapped_users_hierarchies_final_df = mapped_users_hierarchies_final_df.drop_duplicates()

    # From this dataframe we are going to divide the set of users because some of them are equity managers
    # and have to be treated differently
    equity_access_df = mapped_users_hierarchies_final_df[
        (mapped_users_hierarchies_final_df["type hierarchy"] == "Region") &
        (mapped_users_hierarchies_final_df["Franchise or Equity"] == "Equity")
        ]
    # Create a list that contains the equity managers and map them with all the equity regions
    equity_access_list = []
    for user in equity_access_df.user_globalID.unique():
        for hg in equity_access_df.hierarchy_globalID.unique():
            equity_users_dict = {"user": user, "hierarchyGroup": hg}
            equity_access_list.append(equity_users_dict)

    # Create a dataframe that contains just normal assignments, by filtering the previous one
    normal_users_permissions_df = mapped_users_hierarchies_final_df[
        ~mapped_users_hierarchies_final_df.index.isin(equity_access_df.index)
    ]
    normal_access_list = (
        normal_users_permissions_df
        [["user_globalID", "hierarchy_globalID"]]
        .rename(columns={
            "user_globalID": "user",
            "hierarchy_globalID": "hierarchyGroup"
        }
        )
        .to_dict(orient="records")
    )

    final_users_permissions_assignment_list = equity_access_list + normal_access_list

    post_user_permissions_on_api(
        initial_setup=initial_setup,
        permission_mutation=permission_mutation,
        users_and_hierarchies_list=final_users_permissions_assignment_list,
        type_of_user="normal_user"
    )

    return final_users_permissions_assignment_list


def assign_corporate_users_permissions(
        just_loaded_hierarchies_df,
        current_api_users_df,
        initial_setup,
        permission_mutation,
        users_already_assigned,
        post_user_permissions_on_api
):
    """
    Assign corporate users permissions to the new hierarchy groups.
    :param just_loaded_hierarchies_df: The hierarchies dataframe queried from the API
    :param current_api_users_df: The users that already exist in the API
    :param initial_setup: The dictionary containing the connection variables
    :param permission_mutation: The mutation operation
    :param users_already_assigned: Is a list of normal users already assigned. Is used to filer those users
    to not try to give permissions to users twice
    :param post_user_permissions_on_api: The function in charge to post users permissions in the API
    """
    print("\nAssigning corporate user permissions ...\n")
    # Generate a list of corporate users emails
    corporate_users_email_list = pd.read_csv(initial_setup["corporate_users_path"])["Corporate Managers"].tolist()
    corporate_users_email_list = [user.lower() for user in corporate_users_email_list]

    # Create a list of the top level hierarchies
    top_hierarchies = just_loaded_hierarchies_df[just_loaded_hierarchies_df.node_isTop == True]
    highest_hierarchies_global_ids = top_hierarchies.node_id.tolist()

    # Filter the users df in order to get the global ID for the corporate users
    corporate_users_df = current_api_users_df[
        current_api_users_df.node_email.str.lower().isin(corporate_users_email_list)
    ]
    corporate_users_global_ids = corporate_users_df.node_id.tolist()

    mapped_users_with_hierarchies = []
    for user in corporate_users_global_ids:
        for hierarchy in highest_hierarchies_global_ids:
            mapped_user = {
                "user": user,
                "hierarchyGroup": hierarchy
            }
            mapped_users_with_hierarchies.append(mapped_user)

    # Filter the users that have been assigned
    mapped_users_with_hierarchies = [
        item for item in mapped_users_with_hierarchies if item not in users_already_assigned
    ]

    post_user_permissions_on_api(
        initial_setup=initial_setup,
        permission_mutation=permission_mutation,
        users_and_hierarchies_list=mapped_users_with_hierarchies,
        type_of_user="corporate_user"
    )

# GraphQL queries and mutations ---


HIERARCHY_GROUP_PERMISSION_MUTATION = """
mutation hierarchyGroupPermissionAdd ($input: HierarchyGroupPermissionAddInput!) {
        hierarchyGroupPermissionAdd(input: $input) {
            success
        }
}
"""

CREATE_HIERARCHY_LEVELS_MUTATION = """
    mutation hierarchyAddLevels($input: HierarchyAddLevelsInput!){
        hierarchyAddLevels(input: $input) {
            created{
                id
                data
            }
            deleted {
                id
                data
            }
            errors {
                message
                code
            }
            modified{
                id
                data
            }
            success
        }
    }"""
QUERY_HIERARCHY_GROUPS = """
    query currentHierarchyGroups($after:String, $user:GlobalID) {
        hierarchyGroups(after: $after, user: $user) {
            pageInfo {
                hasNextPage
                hasPreviousPage
                startCursor
                endCursor
            }
            edges {
                cursor
                node {
                    id
                    isTop
                    name
                    remoteId
                    label {
                        id
                        name
                    }
                    parent {
                        id
                        isTop
                        name
                    }
                    children {
                        pageInfo {
                            hasNextPage
                            hasPreviousPage
                            startCursor
                            endCursor
                            }
                        edges {
                            cursor
                            node {
                                id
                                isTop
                                name
                            }
                        }
                    }
                }
            }
        }
    }
"""
QUERY_LOCATIONS = """
    query queryLocations($after:String){
        locations (after:$after){
            pageInfo {
                hasNextPage
                hasPreviousPage
                startCursor
                endCursor
            }
            edges {
                cursor
                node {
                    id
                    name
                    remoteId
                    hierarchyGroup {
                        id
                        isTop
                        name
                        
                    }
                }
            }
        }
    }
"""
QUERY_ALL_USERS = """
    query users_query($search: String, $all: Boolean, $after: String) {
        users(search: $search, all: $all, after: $after) {
            pageInfo {
                    hasNextPage
                    hasPreviousPage
                    startCursor
                    endCursor
                }
            edges {
                node {
                    id
                    email
                    firstName
                    lastName
                }
            }
        }
    }
    """

HARD_DELETE_MUTATION = """
    mutation droppingExistingHierarchyGroups($input:HierarchyGroupsHardDeleteInput!) {
        hierarchyGroupsHardDelete(input: $input) {
            success
        }
    }
    """
ASSIGN_LOCATIONS_MUTATION = """
        mutation hierarchyAssignLocations($input: HierarchyAssignLocationsInput!) {
            hierarchyAssignLocations(input: $input) {
                created {
                    id
                }
                errors {
                    message
                    code
                }
                success
            }
        }
        """

MUTATION_ADD_USER = """
        mutation myMutationToCreateUsers($input: UserAddNewToAccountInput!) {
            userAddNewToAccount(input: $input) {
                created {
                    id
                }
                errors {
                    message
                    code
                }
                success
            }
        }
        """

LOAD_NEW_HIERARCHY_GROUPS_MUTATION = """
    mutation hierarchyAddGroups($input: HierarchyAddGroupsInput!) {
        hierarchyAddGroups(input: $input) {
            created {
                id
            }
            errors {
                message
                code
            }
            success
        }
    }
    """

ADD_LOCATIONS_MUTATION = """
    mutation locationAdd($input: LocationAddInput!) {
        locationAdd(input: $input) {
            created {
                id
            }
            errors {
                message
                code
            }
            success
        }
    }
    """
if __name__ == "__main__":
    main()
