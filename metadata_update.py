import os
import pandas as pd
import logging
import httpx
import gql
import stamina
from gql import Client, gql as gql_query
from gql.transport.httpx import HTTPXTransport
from rich.progress import Progress, TextColumn, BarColumn, MofNCompleteColumn
from operator import itemgetter


DTYPES = {
    "num_subjects": "Int64",
    "num_trials": "Int64",
}
AGE_DICT = {
    (0, 10): "0-10",
    (11, 17): "11-17",
    (18, 25): "18-25",
    (26, 34): "26-34",
    (35, 50): "35-50",
    (51, 65): "51-65",
    (66, 1000): "66+",
}

ENDPOINT = "https://openneuro.org/crn/graphql"
QUERY = gql_query("""
query DatasetsWithLatestSnapshots($count: Int, $after: String) {
  datasets(
    first: $count,
    after: $after,
    orderBy: { created: ascending }
    filterBy: { public: true }
  ) {
    edges {
      node {
        id
        publishDate
        latestSnapshot {
          tag
          created
          hexsha
          size
          dataset {
            name
            metadata {
              trialCount
              studyDesign
              studyDomain
              studyLongitudinal
              dataProcessed
              species
              associatedPaperDOI
              openneuroPaperDOI
              dxStatus
              affirmedConsent
              affirmedDefaced
            }
          }
          description {
            SeniorAuthor
            DatasetType
          }
          summary {
            subjects
            modalities
            secondaryModalities
            subjectMetadata {
              age
            }
            tasks
            dataProcessed
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
      count
    }
  }
}
""")

logger = logging.getLogger(__name__)


def format_bool(bool_var: bool) -> str | None:
    if bool_var:
        return "Yes"
    elif bool_var is False:
        return "No"
    else:
        return None


def format_modalities(all_modalities: list) -> str:
    if any(("mri_" in e for e in all_modalities)):
        all_modalities.remove("mri")
    return ", ".join(all_modalities)


def format_ages(raw_age_list: list) -> str | None:
    formatted_list = []
    if raw_age_list:
        age_list = sorted([x["age"] for x in raw_age_list if x["age"]])

        for key, value in AGE_DICT.items():
            if any(x for x in age_list if x >= key[0] and x <= key[1]):
                formatted_list.append(value)

        return ", ".join(formatted_list)
    else:
        return None


def format_name(name: str) -> str | None:
    if not name:
        return None
    elif "," not in name:
        name_list = name.split(" ")
        last = name_list[-1]
        first = " ".join(name_list[:-1])
        new_name = last + ", " + first
        return new_name
    else:
        return name


def handle_error(func, error=TypeError):
    """Returns None if the specified error occurs.
    Otherwise returns output of function."""
    try:
        return func()
    except error:
        return None


@stamina.retry(on=(httpx.HTTPError, gql.transport.exceptions.TransportConnectionFailed))
def get_page(client: Client, count: int, after: str | None) -> dict:
    return client.execute(QUERY, variable_values={"count": count, "after": after})


def get_dataset_count(client: Client) -> int:
    response = get_page(client, 0, None)
    return response["datasets"]["pageInfo"]["count"]


def get_all_metadata(
    client: Client, progress: Progress, task_id, metadata_list
) -> None:
    """Fetches datasets from GraphQL API, processes them and appends to metadata_list"""
    page_info = {"hasNextPage": True, "endCursor": None}

    while page_info["hasNextPage"]:
        try:
            result = get_page(client, 100, page_info["endCursor"])
        except gql.transport.exceptions.TransportQueryError as e:
            logger.error("GraphQL query error")
            if e.data is not None:
                result = e.data

        edges, page_info = itemgetter("edges", "pageInfo")(result["datasets"])

        for edge in edges:
            if edge is None:
                continue

            metadata = create_metadata_dict(edge)
            metadata_list.append(metadata)

            dataset_id = edge["node"]["id"]
            progress.update(task_id, advance=1, dataset=dataset_id)


def create_metadata_dict(in_data: dict) -> dict:
    """Creates a dict containing fields to be outputted for particular dataset."""
    dataset_field = in_data["node"]["latestSnapshot"]["dataset"]
    summary_field = in_data["node"]["latestSnapshot"]["summary"]
    accession_number = in_data["node"]["id"]

    out_data = {
        "accession_number": accession_number,
        "dataset_url": os.path.join(
            "https://openneuro.org/datasets/",
            accession_number,
            "versions",
            in_data["node"]["latestSnapshot"]["tag"],
        ),
        "dataset_name": dataset_field["name"],
        "made_public": handle_error(
            lambda: in_data["node"]["publishDate"][:10],
        ),
        "most_recent_snapshot": in_data["node"]["latestSnapshot"]["created"][:10],
        "num_subjects": handle_error(
            lambda: str(len(summary_field["subjects"])),
        ),
        "modalities": handle_error(
            lambda: format_modalities(
                summary_field["secondaryModalities"] + summary_field["modalities"]
            ),
        ),
        "dx_status": dataset_field["metadata"]["dxStatus"],
        "ages": handle_error(
            lambda: format_ages(summary_field["subjectMetadata"]),
        ),
        "tasks": handle_error(
            lambda: ", ".join(summary_field["tasks"]),
        ),
        "num_trials": dataset_field["metadata"]["trialCount"],
        "study_design": dataset_field["metadata"]["studyDesign"],
        "domain_studied": dataset_field["metadata"]["studyDomain"],
        "longitudinal": format_bool(
            dataset_field["metadata"]["studyLongitudinal"] == "Longitudinal"
        ),
        "processed_data": format_bool(dataset_field["metadata"]["dataProcessed"]),
        "species": dataset_field["metadata"]["species"],
        "nondefaced_consent": format_bool(dataset_field["metadata"]["affirmedConsent"]),
        "affirmed_defaced": format_bool(dataset_field["metadata"]["affirmedDefaced"]),
        "doi_of_papers_from_source_data_lab": dataset_field["metadata"][
            "associatedPaperDOI"
        ],
        "doi_of_paper_published_using_openneuro_dataset": dataset_field["metadata"][
            "openneuroPaperDOI"
        ],
        "senior_author": format_name(
            in_data["node"]["latestSnapshot"]["description"]["SeniorAuthor"]
        ),
        "size_gb": handle_error(
            lambda: round(in_data["node"]["latestSnapshot"]["size"] / (1024**3), 2),
        ),
        "dataset_type": in_data["node"]["latestSnapshot"]["description"]["DatasetType"],
    }

    return out_data


def main() -> int:
    logging.basicConfig()
    client = Client(transport=HTTPXTransport(url=ENDPOINT))

    count = get_dataset_count(client)

    metadata_list = []
    with Progress(
        TextColumn(
            "[progress.description]{task.description} {task.fields[dataset]:8s}"
        ),
        BarColumn(),
        MofNCompleteColumn(),
    ) as progress:
        task = progress.add_task("Fetching", total=count, dataset="...")
        get_all_metadata(client, progress, task, metadata_list)

    df = pd.DataFrame(metadata_list)
    df = df.astype(dtype=DTYPES)
    df = df.set_index("accession_number")
    df = df.sort_index()
    df = df.groupby(df.index).first()  # Remove rows with duplicated accession numbers

    df.to_csv("metadata.csv", mode="w+")


if __name__ == "__main__":
    main()
