import requests
import os
import pandas as pd
import logging
import sys


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
HEADERS = {
    "Accept-Encoding": "gzip, deflate, br",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Connection": "keep-alive",
    "DNT": "1",
    "Origin": "https://openneuro.org",
}

QUERY = """
{
    edges {
        cursor,
        node {
            id,
            publishDate,
            latestSnapshot {
                tag,
                created,
                size,
                dataset {
                    name, 
                    metadata {
                      trialCount,
                      studyDesign,
                      studyDomain,
                      studyLongitudinal,
                      dataProcessed,
                      species,
                      associatedPaperDOI,
                      openneuroPaperDOI,
                      dxStatus,
                      affirmedConsent,
                      affirmedDefaced
                    }
                }, 
                description {
                    SeniorAuthor,
                    DatasetType
                },
                summary {
                    subjects,
                    modalities, 
                    secondaryModalities, 
                    subjectMetadata {
                        age
                    }, 
                    tasks,
                    dataProcessed
                }
            }
        }
    }
}
""".replace("\n", "")


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


def handle_error(func, error):
    """Returns None if the specified error occurs.
    Otherwise returns output of function."""
    try:
        return func()
    except error:
        return None


def perform_query(next_cur=None) -> dict:
    """Performs query and outputs response as dict. If next_cur is not provided,
    gets the first chunk of datasets. Otherwise, gets the next chunk."""
    if next_cur:
        query_dict = (
            f'{{"query": "query testq{{datasets(after: \\"{next_cur}\\") '
            + QUERY
            + '}"}'
        )
    else:
        query_dict = '{"query":"query testq{datasets ' + QUERY + '}"}'

    num_attempts = 3
    for i in range(num_attempts):
        try:
            response = requests.post(
                "https://openneuro.org/crn/graphql", headers=HEADERS, data=query_dict
            )
            return response.json()
        except requests.exceptions.JSONDecodeError:
            pass

    logger.error("Request failed %s times: %s" % (num_attempts, response))
    logger.error("Exiting.")
    sys.exit(1)


def create_data_dict(in_data: dict) -> dict:
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
            TypeError,
        ),
        "most_recent_snapshot": in_data["node"]["latestSnapshot"]["created"][:10],
        "num_subjects": handle_error(
            lambda: str(len(summary_field["subjects"])),
            TypeError,
        ),
        "modalities": handle_error(
            lambda: format_modalities(
                summary_field["secondaryModalities"] + summary_field["modalities"]
            ),
            TypeError,
        ),
        "dx_status": dataset_field["metadata"]["dxStatus"],
        "ages": handle_error(
            lambda: format_ages(summary_field["subjectMetadata"]),
            TypeError,
        ),
        "tasks": handle_error(
            lambda: ", ".join(summary_field["tasks"]),
            TypeError,
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
            TypeError,
        ),
    }

    return out_data


def main():
    logging.basicConfig()

    # Initial query
    response = perform_query()

    output = []
    while True:
        ds_data_list = response["data"]["datasets"]["edges"]

        for ds_data in ds_data_list:
            if not ds_data:
                continue

            row = create_data_dict(ds_data)
            output.append(row)

        # A response with < 25 datasets implies last query
        if len(ds_data_list) < 25:
            break

        # Next query
        response = perform_query(next_cur=ds_data_list[-1]["cursor"])

    df = pd.DataFrame(output)
    df = df.astype(dtype=DTYPES)
    df = df.set_index("accession_number")
    df = df.sort_index()
    df = df.groupby(df.index).first()  # Remove rows with duplicated accession numbers
    df.to_csv("metadata.csv", mode="w+")


if __name__ == "__main__":
    main()
